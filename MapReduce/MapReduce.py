from mrjob.job import MRJob

class MRHotelRatingCount(MRJob):
    def mapper(self, _, line):
        (userId, movieId, rating, _) = line.split(",")

        try:
            result = [movieId, float(rating)]
            yield result
        except (ValueError, TypeError) as e:
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
