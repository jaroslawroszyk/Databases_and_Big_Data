from mrjob.job import MRJob

class MRHotelRatingCount(MRJob):
    def mapper(self, _, line):
        (_, movieId, rating, _) = line.split(",")

        try:
            result = [movieId, float(rating)]
            yield result
        except (ValueError, TypeError):
            pass

    def reducer(self, movie_id, ratings):
        total_ratings = 0
        num_ratings = 0

        for rating in ratings:
            total_ratings += rating
            num_ratings += 1

        average_rating = total_ratings / num_ratings

        yield movie_id, average_rating

if __name__ == '__main__':
    MRHotelRatingCount.run()


Zmodyfikuj skrypt z poprzedniego ćwiczenia, aby łączył dwa pliki ratings i movies. Jego wynikiem powinno być zestawienie średnich ocen wraz z tytułem filmu


moviesWith3.csv:
 movieId,title,genres
1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
2,Jumanji (1995),Adventure|Children|Fantasy
3,Grumpier Old Men (1995),Comedy|Romance

raitingsWith3.csv:

userId,movieId,rating,timestamp
1,1,4.0,964982703
1,2,5.0,964982703
1,3,3.0,964981247


