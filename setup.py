"""
使用Cython将python项目代码编译为so，用于项目加密部署。

要求：
    代码应遵循PEP8。
    待编译的代码中不含type/isinstance类型检测函数，如必须含有，请将类型检测移出到程序入口文件中。
    
使用前应修改以下变量值：
    sources: 源文件目录列表。（请填写相对路径，不能使用绝对路径！）
    target_dir: 目标文件夹，源文件将复制到目标文件夹进行编译。
    compiling_exclude_files: 不编译的文件列表，这些文件将不编译，如程序入口文件。（请填写文件相对路径，不能使用绝对路径！）
    files_not_copy: 不复制的文件列表，这些文件将不复制到目标文件夹。（请填写文件相对路径，不能使用绝对路径！）

过程：
    1. 扫描文件夹中的所有py文件（忽略排除的文件）.
    2. 使用Cython将py文件转换为C，并编译为so.
    3. 删除已编译的py文件.
    
执行编译：
    python setup.py build_ext --inplace
"""
import os
import shutil
import re
import sys
import logging
from pathlib import Path
from distutils.core import Extension, setup
from Cython.Build import cythonize
from Cython.Compiler import Options

try:
    root_dir = Path(__file__).parent.absolute()
except:
    root_dir = Path('.').absolute()
    
# 配置项
# 源文件目录列表
# 请填写相对路径，不能使用绝对路径！
sources = ['./']

# 不编译的文件列表
# 请填写文件相对路径，不能使用绝对路径！
compiling_exclude_files = [
    "./data_prep2.py",
    "./main.py", 
    "./distributed_worker.py"
]

# 不复制的文件列表
# 请填写文件相对路径，不能使用绝对路径!
# 可使用"**/filename"表示任意文件夹下的"filename"文件
files_not_copy = [
    './.old', './old', './log',                                 
    './saved', './others', './tests'
]

# 检查格式是否符合要求
print('检查路径格式：')
for f in sources:
    print('源文件目录：', f)
    assert not f.startswith('/') and ':\\' not in f, \
        "请不要填写绝对路径！"
for f in compiling_exclude_files:
    print('不编译的文件：', f)
    assert not f.startswith('/') and ':\\' not in f, \
        "请不要填写绝对路径！"
for f in files_not_copy:
    print('不复制的文件：', f)
    assert not f.startswith('/') and ':\\' not in f, \
        "请不要填写绝对路径！"
    
    # 文件路径结尾不得以“/”或“\\”结尾
    if f.endswith('/'):
        logging.warning('不复制的文件列表中的文件路径结尾不得以“/”结尾！')
        f = f[:-1]
    if f.endswith('\\'):
        logging.warning('不复制的文件列表中的文件路径结尾不得以“\\”结尾！')
        f = f[:-2]

# 目标文件夹
target_dir = '.deploy/'
target_dir = os.path.abspath(os.path.join(root_dir, target_dir))
print('Target dir: {}'.format(target_dir))

# 目标路径存在时是否删除
if os.path.exists(target_dir):
    logging.warning('目标文件夹{}已存在！'.format(target_dir))
    flag = input('是否删除{}，请输入yes/no:'.format(target_dir)).lower().strip()
    while flag not in ('yes', 'no'):
        flag = input('是否删除{}，请输入yes/no:'.format(target_dir)).lower().strip()
    if flag == 'yes':
        shutil.rmtree(target_dir)
    else:
        sys.exit()
        
# 默认不复制的文件列表
files_not_copy_default = [
    '**/.git', '**/.idea', '**/.vscode', '**/nohup.out',        
    '**/.ipynb_checkpoints', '**/__pycache__', './.deploy'
]
# 不复制的文件列表合并
files_not_copy.extend(files_not_copy_default)

# 不复制的文件名（任意文件夹中）
file_names_not_copy = [
    f[3:] 
    for f in files_not_copy
    if f.startswith('**/')
]     
print('不复制的文件名（任意文件夹中）: {}'.format(file_names_not_copy))
 
# 其他不复制的文件绝对路径
file_paths_not_copy = [
    os.path.abspath(f)
    for f in files_not_copy
    if not f.startswith('**/')
]
print('其他不复制的文件绝对路径：{}'.format(file_paths_not_copy))
        
 
def ignore_paths():
    """
    输入要忽略的路径，返回shutil.copytree函数ignore的输入值
    """
    def ignoref(directory, contents):
        return [
            f for f in contents 
            if f in file_names_not_copy or os.path.abspath(
                os.path.join(directory, f)) in file_paths_not_copy
        ]
    return ignoref
    
    
# 复制到目标文件夹，注意忽略文件列表
for i in range(len(sources)):
    shutil.copytree(
        sources[i], 
        os.path.join(target_dir, sources[i]),
        ignore=ignore_paths()
    )
    
# 切换到目标文件夹，在该目录下进行操作
os.chdir(target_dir)

# 编译排除文件列表中的文件路径改为绝对路径
compiling_exclude_files = [
    str(Path(f).absolute()) for f in compiling_exclude_files
]
print('不编译的文件绝对路径: {}'.format(compiling_exclude_files))

# 待删除的py文件列表
drop_pyfiles = []
# 扫描文件夹中的py文件
for source in sources:
    extensions = []
    print('source dir: {}'.format(source))
    cwd = os.getcwd()
    os.chdir(source)  # 切换路径
    # 扫描py
    for p in Path('./').glob('**/*'): 
        if p.suffix not in ('.py', '.pyc'):
            continue
        if p.name in ('__init__.py', 'setup.py'):
            continue
        if str(p.absolute()) in compiling_exclude_files:
            continue 

        print(p.absolute())            
                    
        # 检查是否含有type/isinstance类型检测函数
        with open(p, 'r', encoding='utf8') as fp:
            content = fp.read()
        content = re.split(
            r'if[\s]+__name__[\s]*==[\s]*[\"\']__main__[\"\']', content)[0]
        flag = 0
        if re.search(r'type\(', content):
            logging.warning('{}中使用了type类型检测函数!'.format(p))
            flag = 1
        if re.search(r'isinstance\(', content):
            logging.warning('{}中使用了isinstance函数!'.format(p))
            flag = 1
        if flag:
            flag = input('输入1跳过当前文件编译，输入2仍然编译：').lower().strip()
            while flag not in ('1', '2'):
                flag = input('输入1跳过当前文件编译，输入2仍然编译：').lower().strip()
            if flag == '1':
                continue
            else:
                pass
        
        # 模块名
        # module_name = str(p.parent.joinpath(p.stem)).replace(
        #     '/', '.').replace('\\', '.')
        # while module_name.startswith('.'):
        #     module_name = module_name[1:]
        module_name = p.stem
            
        # 添加待编译模块
        extensions.append(
            Extension(
                module_name, 
                [str(p)], 
                extra_compile_args=["-Os", "-g0"],
                extra_link_args=["-Wl,--strip-all"]
            )
        )
        
        # 添加到待删除的py文件列表
        drop_pyfiles.append(p.absolute())
        
    # 编译
    print("debug point 1")
    Options.docstrings = False
    compiler_directives = {'optimize.unpack_method_calls': False}
    setup(  
        ext_modules=cythonize(
            extensions, exclude=None, 
            nthreads=20, quiet=True, build_dir='./build',
            language_level=3, compiler_directives=compiler_directives)
    )
                                    
    shutil.rmtree('./build/')  # 删除build文件夹    
    os.chdir(cwd)
    
print('编译完成。') 

# 删除py文件
print('删除py文件...')
for pyfile in drop_pyfiles:
    os.remove(pyfile)