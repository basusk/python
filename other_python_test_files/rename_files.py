import os

def rename_file():
    file_list = os.listdir(r"C:\Python27\files_for_renaming")
    print (file_list)
    path = os.getcwd()
    os.chdir(r"C:\Python27\files_for_renaming")

    for file_name in file_list:
        os.rename(file_name, file_name.translate(None, "0123456789"))
    os.chdir(path)
rename_file()
