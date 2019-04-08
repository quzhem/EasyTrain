from utils.Log import Log
import os
import subprocess
import re


def test(projectName=None, basePath='/Users/bjqxdn0702/IdeaProjects', branchs=[], pickVersion=None):
    # val = os.system("cd ~/IdeaProjects/jingdata-paas-metadata/")
    # val = os.system('git pull')
    # val = os.system('git push new --all')
    # Log.v(val)
    # subprocess.call('cd ~/IdeaProjects/jingdata-paas-metadata/ && git push new --all', shell=True)
    if (branchs == None or pickVersion == None):
        return

    Log.v("start %s " % projectName)
    for branch in branchs:
        path = basePath + "/" + projectName
        checkResult = excute(path, 'git checkout ' + branch)
        if (re.match('Already on \'%s\'|.* branch is up to date with .*|.*Your branch is ahead.*|Your branch is behind .*' % branch,
                     checkResult)):
            Log.v(checkResult)
        else:
            raise ValueError("checkout error %s", checkResult)
        excute(path, 'git pull ')
        cherryPickResult = excute(path, 'git cherry-pick ' + pickVersion)
        Log.v("pick result %s" % cherryPickResult)
        excute(path, 'git push ')

        # subprocess.check_call('git pull ', shell=True, cwd=basePath + "/" + projectName)
        # subprocess.check_call('git checkout ' + branch, shell=True, cwd=basePath + "/" + projectName)
        # subprocess.check_call('git cherry-pick ' + pickVersion, shell=True, cwd=basePath + "/" + projectName)
        # subprocess.check_call('git push new --all ', shell=True, cwd=basePath + "/" + projectName)


def excute(cwd, shell):
    p = subprocess.run(shell, cwd=cwd, shell=True, stdout=subprocess.PIPE, check=True)
    # p = subprocess.Popen(shell, cwd=cwd, shell=True, stdout=subprocess.PIPE)
    # p.wait()
    # out, error = p.communicate()
    return p.stdout.decode()


def main():
    test('jingdata-saas-investment', branchs=['dev-3.2-fund'], pickVersion='07751c7')
    Log.v("全部执行完成")


if __name__ == '__main__':
    main()
