import random

def getNumber():
    return random.randint(2, 15)

print("Begin")

a = getNumber()
b = getNumber()
print(str(a)+" + "+str(b)+" = ?")    
text = input()
counter = 0

while (text != 'end'):
    if (text.isdigit() ):
        if ( a+b == int(text.strip()) ):
            counter += 1
            a = getNumber()
            b = getNumber()
            print()
            print(f'Отлично! Молодец! Задач решено: {counter}')
            print(str(a)+" + "+str(b)+" = ?")
        else:
            print("Попробуй еще раз")
            print(str(a)+" + "+str(b)+" = ?")
    else:
        print("Пожалуйста, напечатай только цифры. Попробуй еще раз")
        print(str(a)+" + "+str(b)+" = ?")
        
    text = input()