from mrjob.job import MRJob
from mrjob.protocol import TextProtocol

class MRRatingAverage(MRJob):

    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(MRRatingAverage, self).configure_args()
        self.add_file_arg('--movies', help='path to movies.csv')

    def mapper_init(self):
        self.movie_data = {}
        with open(self.options.movies, 'r', encoding='utf-8') as movies_file:
            for line in movies_file:
                fields = line.strip().split(',')
                if len(fields) >= 2:
                    movie_id = fields[0]
                    movie_title = fields[1]
                    self.movie_data[movie_id] = movie_title

    def mapper(self, _, line):
        if line.startswith("userId"):
            return

        fields = line.split(',')
        movie_id = fields[1]
        rating = float(fields[2])

        if movie_id in self.movie_data:
            yield self.movie_data[movie_id], rating

    def reducer(self, movie_title, ratings):
        ratings_list = list(ratings)
        total_ratings = len(ratings_list)
        if total_ratings > 0:
            average_rating = sum(ratings_list) / total_ratings
            average_rating = round(sum(ratings_list) / total_ratings, 2)
            yield None, f'{movie_title}: {average_rating}'

if __name__ == '__main__':
    MRRatingAverage.run()
