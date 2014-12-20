 This directory contains various sсripts needed to deploy and install the project.
List of actions to deploy is:
1. To prepare on target machine build directory with its subdirectories:
(look on deploy scripts what is needed ...)
2. After building ImpalaToGo you should put deploy/deploy.sh one level above ImpalaHome and run it.
Two parameters - target machine and pem key

3. On target machnie put all scripts from the installScripts directory into your build directory
4. copy install/impalaScripts directory under build directory
4. Run install.sh script on target machine.

5. Put scripts from templates directory to the same level as build directory, and rename it to conf
6. put install/ImpalaToGo scripts on the level above build directory

7. From here you can use how-to-try wiki page

