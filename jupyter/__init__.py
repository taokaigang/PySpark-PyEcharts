
# pip3 install --upgrade pip
# pip3 install jupyter

# 或者使用 conda install jupyter notebook

# 帮助 jupyter notebook --help 或者 jupyter notebook -h

# 启动 jupyter notebook 默认浏览器 http://localhost:8891/tree
# 指定端口启动 jupyter notebook --port <port_number>


# 把venv/bin/jupyter-notebook里面的内容copy到这里来启动jupyter-notebook
#!/Users/kgt/code/github/PySpark-PyEcharts/venv/bin/python
# -*- coding: utf-8 -*-
import re
import sys
from notebook.notebookapp import main
if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw|\.exe)?$', '', sys.argv[0])
    sys.exit(main())

