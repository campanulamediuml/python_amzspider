import os


def get_pyx():
    file_list = os.listdir('/Users/JingjingHe/Desktop/python_amzspider_private/cython_test')
    for name in file_list:
        if name[-3:] == '.py' and name != 'test.py' :
            open(name+'x','w').write(open(name).read())
            if name != 'main_run.py':
                os.system('rm '+name)
    try:
        os.system('rm main_run.pyx')
    except:
        pass


def write_setup():
    file_list = os.listdir('/Users/JingjingHe/Desktop/python_amzspider_private/cython_test')
    fh = open('setup.py','w')
    fh.write('from distutils.core import setup\nfrom Cython.Build import cythonize\n')
    for name in file_list:
        if name[-4:] == '.pyx' and name != 'setup.pyx':
            fh.write('setup(ext_modules=cythonize("'+name+'"))\n')

def pyx_to_py():
    file_list = os.listdir('/Users/JingjingHe/Desktop/python_amzspider_private/cython_test')
    for name in file_list:
        if name[-4:] == '.pyx':
            open(name[:-1],'w').write(open(name).read())
            os.system('rm '+name)
    os.system('rm *.c')
    os.system('rm *.so')
    os.system('rm -rf build')
    

# get_pyx()
# write_setup()
# # pyx_to_py()
import word_key
import washdb
washdb.main()
word_key.main()


