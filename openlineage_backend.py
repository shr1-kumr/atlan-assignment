from flask import Flask, request, jsonify
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime

# Initialize Flask app
app = Flask(__name__)

# Configure SQLite in-memory database
engine = create_engine('sqlite:///example.db')
Session = sessionmaker(bind=engine)
Base = declarative_base()


# Define data models
class JobRun(Base):
    __tablename__ = 'job_runs'

    run_id = Column(String, primary_key=True)
    name = Column(String)
    created_at = Column(String, default=datetime.utcnow().isoformat())
    inputs = relationship('Dataset', back_populates='job_run_inputs', lazy='dynamic')
    outputs = relationship('Dataset', back_populates='job_run_outputs', lazy='dynamic')


class Dataset(Base):
    __tablename__ = 'datasets'

    dataset_id = Column(String, primary_key=True)
    name = Column(String)
    job_run_id = Column(String, ForeignKey('job_runs.run_id'))
    job_run_inputs = relationship('JobRun', back_populates='inputs')
    job_run_outputs = relationship('JobRun', back_populates='outputs')


# Create tables
Base.metadata.create_all(bind=engine)


# Define API route to receive OpenLineage events
@app.route('/api/v1/lineage', methods=['POST'])
def receive_openlineage_event():
    data = request.json
    print(data)
    process_openlineage_event(data)
    return jsonify({'message': 'Event processed successfully'})


def process_openlineage_event(data):
    with Session() as session:
        run_id = data['run']['runId']
        job_run = session.query(JobRun).filter_by(run_id=run_id).first()
        if not job_run:
            job_run = JobRun(run_id=run_id, name=data['job']['name'])
            session.add(job_run)

        for input_dataset in data.get('inputs', []):
            dataset_id = input_dataset['name']
            dataset = session.query(Dataset).filter_by(dataset_id=dataset_id).first()
            if not dataset:
                dataset = Dataset(dataset_id=dataset_id, name=input_dataset['name'])
                session.add(dataset)
            job_run.inputs.append(dataset)

        for output_dataset in data.get('outputs', []):
            dataset_id = output_dataset['name']
            dataset = session.query(Dataset).filter_by(dataset_id=dataset_id).first()
            if not dataset:
                dataset = Dataset(dataset_id=dataset_id, name=output_dataset['name'])
                session.add(dataset)
            job_run.outputs.append(dataset)

        session.commit()


@app.route('/api/v1/lineage/<run_id>', methods=['GET'])
def get_lineage(run_id):
    lineage_graph = build_lineage_graph(run_id)
    return jsonify({'lineage': lineage_graph})


@app.route('/health', methods=['GET'])
def health():
    return 'OK'


def build_lineage_graph(run_id):
    lineage_graph = {}

    with Session() as session:
        job_run = session.query(JobRun).filter_by(run_id=run_id).first()

        if job_run:
            lineage_graph['run_id'] = job_run.run_id
            lineage_graph['job_name'] = job_run.name
            lineage_graph['inputs'] = [input_dataset.dataset_id for input_dataset in job_run.inputs]
            lineage_graph['outputs'] = [output_dataset.dataset_id for output_dataset in job_run.outputs]

    return lineage_graph


if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5001, debug=True)
