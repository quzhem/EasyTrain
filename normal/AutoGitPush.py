from utils.Log import Log
import os
import subprocess


def test(projects=[], basePath='/Users/bjqxdn0702/IdeaProjects'):
    # val = os.system("cd ~/IdeaProjects/jingdata-paas-metadata/")
    # val = os.system('git pull')
    # val = os.system('git push new --all')
    # Log.v(val)
    # subprocess.call('cd ~/IdeaProjects/jingdata-paas-metadata/ && git push new --all', shell=True)
    # subprocess.call('cd ~/IdeaProjects/jingdata-paas-metadata/ && git push new --all', shell=True)
    for projectName in projects:
        Log.v("start %s " % projectName)
        subprocess.check_call('git pull', shell=True, cwd=basePath + "/" + projectName)
        subprocess.check_call('git push new --all ', shell=True, cwd=basePath + "/" + projectName)


def main():
    #'jingdata-api-gateway',
    test(
        ['jingdata-paas-metadata', 'jingdata-paas-message', 'jingdata-paas-search',  'jingdata-paas-org',
         'jingdata-paas-auth', 'jingdata-paas-datarights', 'jingdata-paas-log-collection', 'jingdata-saas-investment', 'jingdata-paas-log',
         'jingdata-paas-common', 'jingdata-paas-common-extend', 'jingdata-paas-common-strength', 'jingdata-paas-common-basic'])
    test(['jingdata-admin-web'], '/Users/bjqxdn0702/Documents/36kr')
    Log.v("全部执行完成")


if __name__ == '__main__':
    main()
