from ariadne import ObjectType

from mutation.resolver import save_job, save_user, initialize_tables

custom_mutation = ObjectType("Mutation")

custom_mutation.set_field('save_job', save_job)
custom_mutation.set_field('save_user', save_user)
custom_mutation.set_field('init_tables', initialize_tables)
