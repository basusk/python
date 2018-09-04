import turtle

window = turtle.Screen()
window.bgcolor("red")

def draw_square():
    brad = turtle.Turtle()
    brad.shape("turtle")
    brad.color("yellow")
    brad.speed(2)

    totalTurns = 4
    numberOfTurns = 0

    while(numberOfTurns < totalTurns):
        brad.forward(100)
        brad.right(90)
        numberOfTurns = numberOfTurns + 1

def draw_circle():
    angie = turtle.Turtle()
    angie.shape("arrow")
    angie.color("blue")
    angie.circle(100)

def draw_triangle():
    tom = turtle.Turtle()
    tom.color("black")
    numberOfTurns = 0
    totalTurns = 3

    while(numberOfTurns < totalTurns):
        tom.backward(100)
        tom.left(120)
        numberOfTurns = numberOfTurns + 1

    window.exitonclick()

draw_square()
draw_circle()
draw_triangle()
