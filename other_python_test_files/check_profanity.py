from profanity import profanity

def read_text():
    quotes = open(r"C:\Users\Saikat\Documents\SavedPyFiles\test.txt")
    contents_of_file = quotes.read()
    print(contents_of_file)
    output = profanity.contains_profanity(contents_of_file)
    #print(output)
    if output == True:
        print("Alert: This file is not good")
    elif output == False:
        print("No problem, file is good")
    else:
        print("Could not scan the file")
    quotes.close()

read_text()
