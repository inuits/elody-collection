from cron_jobs.ttl_checker import TtlChecker
from flask import request
from inuits_policy_based_auth import RequestContext
from policy_factory import apply_policies
from resources.base_resource import BaseResource



class Cron(BaseResource):
    @apply_policies(RequestContext(request))
    def get(self):
        checker = TtlChecker()
        checker()
        return None, 200
