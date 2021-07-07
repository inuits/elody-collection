# Job-status App entry Point

from configuration.config import jobs
from view import views_controller

if __name__ == "__main__":
    jobs.run()
