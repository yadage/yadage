import os
import subprocess
import sys
import time
import psutil
import utils

handlers,environment = utils.handler_decorator()

@environment('docker-encapsulated')
class docker_enc_handler(object):
    def __init__(self,nametag,context,cmd,environment_spec,log):
        self.nametag  = nametag
        self.command  = cmd
        self.spec = environment_spec
        self.log  = log
        self.context = context
    def handle(self):
        environment = self.spec
        log = self.log
        workdir = self.context['workdir']
        container = self.spec['image']
  
  
        report = '''\n\
--------------
run in docker container: {container}
with env: {env}
command: {command}
resources: {resources}
--------------
      '''.format( container = container,
                  command = self.command,
                  env = environment['envscript'] if environment['envscript'] else 'default env',
                  resources = environment['resources']
                )
  
        do_cvmfs = 'CVMFS' in environment['resources']
        do_grid  = 'GRIDProxy'  in environment['resources']
  
        log.info(report)
        log.info('dogrid: {} do_cvmfs: {}'.format(do_grid,do_cvmfs))

        envmod = 'source {} &&'.format(environment['envscript']) if environment['envscript'] else ''

        in_docker_cmd = '{envmodifier} {command}'.format(envmodifier = envmod, command = self.command)

        docker_mod = '-v {}:/workdir'.format(os.path.abspath(self.context['global_workdir']))
        if do_cvmfs:
            if not 'RECAST_CVMFS_LOCATION' in os.environ:
                docker_mod+=' -v /cvmfs:/cvmfs'
            else:
                docker_mod+=' -v {}:/cvmfs'.format(os.environ['RECAST_CVMFS_LOCATION'])
        if do_grid:
            if not 'RECAST_AUTH_LOCATION' in os.environ:
                docker_mod+=' -v /home/recast/recast_auth:/recast_auth'
            else:
                docker_mod+=' -v {}:/recast_auth'.format(os.environ['RECAST_AUTH_LOCATION'])

        in_docker_host = 'echo $(hostname) > /workdir/{nodename}.hostname && {cmd}'.format(nodename = self.nametag, cmd = in_docker_cmd)

        fullest_command = 'docker run --rm {docker_mod} {container} sh -c \'{in_dock}\''.format(docker_mod = docker_mod, container = container, in_dock = in_docker_host)
        if do_cvmfs:
            fullest_command = 'cvmfs_config probe && {}'.format(fullest_command)
          # fullest_command = 'eval $(docker-machine env default) && echo cvmfs_config probe && {}'.format(fullest_command)


        log.info('context: \n {}'.format(self.context))

        docker_pull_cmd = 'docker pull {container}'.format(container = container)

        log.info('docker pull command: \n  {}'.format(docker_pull_cmd))
        log.info('docker run  command: \n  {}'.format(fullest_command))

        try:
          with open('{}/{}.pull.log'.format(workdir,self.nametag),'w') as logfile:
            proc = subprocess.Popen(docker_pull_cmd,shell = True, stderr = subprocess.STDOUT, stdout = logfile)
            log.info('started pull subprocess with pid {}. now wait to finish'.format(proc.pid))
            time.sleep(0.5)
            log.info('process children: {}'.format([x for x in psutil.Process(proc.pid).children(recursive = True)]))
            proc.communicate()
            log.info('pull subprocess finished. return code: {}'.format(proc.returncode))
            if proc.returncode:
              log.error('non-zero return code raising exception')
              raise subprocess.CalledProcessError(returncode =  proc.returncode, cmd = docker_pull_cmd)
            log.info('moving on from pull')
        except RuntimeError as e:
          log.info('caught RuntimeError')
          raise e
        except subprocess.CalledProcessError as exc:
          log.error('subprocess failed. code: {},  command {}'.format(exc.returncode,exc.cmd))
          raise RuntimeError('failed docker subprocess in runStep.')
        except:
          log.info("Unexpected error: {}".format(sys.exc_info()))
          raise
        finally:
          log.info('finally for pull')

        try:
            with open('{}/{}.run.log'.format(workdir,self.nametag),'w') as logfile:
                proc = subprocess.Popen(fullest_command,shell = True, stderr = subprocess.STDOUT, stdout = logfile)
                log.info('started run subprocess with pid {}. now wait to finish'.format(proc.pid))
                time.sleep(0.5)
                log.info('process children: {}'.format([x for x in psutil.Process(proc.pid).children(recursive = True)]))
                proc.communicate()
                log.info('pull subprocess finished. return code: {}'.format(proc.returncode))
                if proc.returncode:
                    log.error('non-zero return code raising exception')
                    raise subprocess.CalledProcessError(returncode =  proc.returncode, cmd = fullest_command)
                log.info('moving on from run')
        except subprocess.CalledProcessError as exc:
            log.error('subprocess failed. code: {},  command {}'.format(exc.returncode,exc.cmd))
            raise RuntimeError('failed docker subprocess in runStep.')
        except:
            log.error("Unexpected error: {}".format(sys.exc_info()))
            raise
        finally:
            log.info('finally for run')
        log.info('reached return for runStep')