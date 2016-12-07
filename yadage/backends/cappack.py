from packtivity.handlers.environment_handlers import environment
import logging
log = logging.getLogger('yadage.cap')

@environment('docker-encapsulated','cap')
def run_in_env(environment,context,job):
    log.info('running an job for CAP!!')
