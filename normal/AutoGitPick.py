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
        excute(path, 'git pull ')
        checkResult = excute(path, 'git checkout ' + branch)
        if (re.match('.* branch is up to date with \'origin/%s\'.*|.*Your branch is ahead.*' % branch, checkResult)):
            Log.v(checkResult)
        else:
            raise ValueError("checkout error %s", checkResult)
        cherryPickResult = excute(path, 'git cherry-pick ' + pickVersion)
        if (re.match('.* file changed, .* insertion.*, .* deletion(.*).*' % branch, checkResult)):
            Log.v(checkResult)
        else:
            raise ValueError("checkout error %s", checkResult)
        Log.v(cherryPickResult)
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
    test('jingdata-saas-investment', branchs=['lvdi'], pickVersion='6a3586b')
    Log.v("全部执行完成")


if __name__ == '__main__':
    main()
