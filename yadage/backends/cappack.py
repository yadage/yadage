from packtivity.handlers.environment_handlers import environment

@environment('docker-encapsulated','cap')
def run_in_env(environment,context,job):
    print 'running an job for CAP!!'
