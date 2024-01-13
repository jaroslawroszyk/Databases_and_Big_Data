from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMovieRatings(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(mapper=self.mapper_get_titles,
                   mapper_init=self.mapper_init,
                   reducer=self.reducer_add_titles)
        ]

    def mapper_get_ratings(self, _, line):
        # Split the line from ratingsWith3.csv
        (userId, movieId, rating, timestamp) = line.split(",")
        try:
            yield movieId, float(rating)
        except ValueError:
            # Skip lines with parsing errors
            pass

    def reducer_count_ratings(self, movieId, ratings):
        total_ratings = sum(ratings)
        num_ratings = len(ratings)
        average_rating = total_ratings / num_ratings
        # Emit movieId and average rating
        yield movieId, (average_rating, num_ratings)

    def mapper_init(self):
        # Load movies data
        self.movie_titles = {}

        with open("moviesWith3.csv", "r") as f:
            for line in f:
                if line.strip() != '':
                    parts = line.split(",")
                    movieId = parts[0].strip()
                    title = parts[1].strip()
                    self.movie_titles[movieId] = title

    def mapper_get_titles(self, movieId, movie_info):
        # Emit movie title with the rating info
        yield self.movie_titles.get(movieId), movie_info

    def reducer_add_titles(self, title, movie_infos):
        for movie_info in movie_infos:
            yield title, movie_info

if __name__ == '__main__':
    MRMovieRatings.run()
