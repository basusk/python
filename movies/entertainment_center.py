import media
import fresh_tomatoes

toy_story = media.Movie("Toy Story",
                        "A story of a boy and his toys come to life",
                        "https://upload.wikimedia.org/wikipedia/en/1/13/Toy_Story.jpg",
                        "https://www.youtube.com/watch?v=KYz2wyBy3kc")

#print(toy_story.storyline)

avatar = media.Movie("Avatar",
                     "A marine on an alien planet",
                     "https://upload.wikimedia.org/wikipedia/sco/b/b0/Avatar-Teaser-Poster.jpg",
                     "https://www.youtube.com/watch?v=6ziBFh3V1aM")
                     
#print(avatar.storyline)
#avatar.show_trailer()

school_of_rock = media.Movie("School_of_Rock", "Using rock music to learn",
                             "https://upload.wikimedia.org/wikipedia/en/1/11/School_of_Rock_Poster.jpg",
                             "https://www.youtube.com/watch?v=XCwy6lW5Ixc")

ratatouille = media.Movie("Ratatouille", "A rat is a chef in Paris",
                          "https://upload.wikimedia.org/wikipedia/en/5/50/RatatouillePoster.jpg",
                          "https://www.youtube.com/watch?v=ALUmKa_mpik")

sonar_kella = media.Movie("The Golden Fortess", "Story of a boy able to remember events of his previous life",
                          "https://upload.wikimedia.org/wikipedia/en/f/f3/Dvd_sonar_kella_ray.jpg",
                          "https://www.youtube.com/watch?v=rNkeVit2560")

joi_baba_felunath = media.Movie("The Elephant God","Story of recovering and finding thief of a valuable element statue",
                                "https://upload.wikimedia.org/wikipedia/en/7/78/Joybaba_phelunath_re.jpg",
                                "https://www.youtube.com/watch?v=mJ2nQAXMsM4")


movies = [toy_story, sonar_kella, avatar, school_of_rock, joi_baba_felunath, ratatouille]
fresh_tomatoes.open_movies_page(movies)
print(media.Movie.__module__)
