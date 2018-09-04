import turtle

window = turtle.Screen()
window.bgcolor("red")


def draw_square(some_turtle):
    for i in range(1,5):
        some_turtle.forward(100)
        some_turtle.right(90)


def draw_art():
    brad = turtle.Turtle()
    brad.shape("turtle")
    brad.color("blue")
    brad.speed(2)
    brad.pu()
    brad.sety(150)
    brad.pd()
    for i in range (1,37):   
        draw_square(brad)
        brad.right(10)


def draw_signature():
    saikat = turtle.Turtle()
    
    basu = turtle.Turtle()
    
    saikat.shape("arrow")
    saikat.color("black")
    saikat.speed(2)
    saikat.backward(100)
    saikat.left(270)
    saikat.forward(100)
    saikat.left(90)
    saikat.forward(100)
    saikat.right(90)
    saikat.forward(100)
    saikat.right(90)
    saikat.forward(100)

    basu.shape("arrow")
    basu.color("black")
    basu.speed(2)
    basu.pu()
    basu.forward(20)
    basu.pd()
    basu.right(90)
    basu.forward(200)
    basu.left(90)
    basu.forward(100)
    basu.left(90)
    basu.forward(100)
    basu.left(90)
    basu.forward(100)
    basu.pu()
    basu.right(90)
    basu.forward(100)
    basu.pd()
    basu.right(90)
    basu.forward(100)
    basu.right(90)
    basu.forward(100)
    
    window.exitonclick()

draw_art()
draw_signature()
