# Job Status API System 

Project requirements 
    
    Small task to Implement job status-system

    end-points to retrieve status:
    per job-id
    jobs for user
    per asset
    
    create a job entity with fields for:
    start & end timestamps
    user (optional)
    asset (optional)
    job-id (uuid)
    mediafile-id (optional)
    job-type
    job-info
    status (enum, for now: queued, in-progress, finished, failed)

## Requirements destructuring 

from the requirement provided above, I have  designed an API system that server three endpoints for jobs entity. 
There will also be an additional entity which will be for Users. Since every job created needs to be associated with a User who created it, 
there will be a one to many relationships between these entities (Job and  User). 

Initial authentication of the job-status system users wil be provided and implemented at a small scale which will be 
upgraded later on to use SSO And 2FA. 

## Server Setup

 By default, this server `runs on port 5600` which can be changed inside `main.py` or when running inside a `wsgi` environment using `gunicorn` or using `docker-compose`
 
## API Endpoints structure
    

    