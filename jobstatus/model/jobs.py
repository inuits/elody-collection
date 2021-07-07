import enum
import uuid

from configuration.config import entity as job


class JobStatus(enum.Enum):
    """ JobStatus Enumeration """
    QUEUED = "Queued"
    PROGRESS = 'Progress'
    FINISHED = 'Finished'
    FAILED = 'Failed'


class Job(job.Model):
    """
    Job entity - save jobs and maps (one to many)  to User entity
    @author SK
    """
    __tablename__ = 'Job'
    __bind_key__ = 'JOB_DB'
    id = job.Column(job.Integer, primary_key=True)
    job_id = job.Column(job.String(250), nullable=False, unique=True)  # uuid string.
    start_date = job.Column(job.TIMESTAMP, nullable=False)
    end_date = job.Column(job.TIMESTAMP, nullable=False)
    owner = job.Column(job.Integer, job.ForeignKey('User.user_id'))  # foreign key mapping to User table.
    asset = job.Column(job.String(200))
    mediafile_id = job.Column(job.String(200))
    job_type = job.Column(job.String(200))
    # two options to implement this enum, from graphQL or within this ORM.
    status = job.Column(job.Enum(JobStatus, values_callable=lambda obj: [e.value for e in obj]))
    job_info = job.Column(job.Text)
