from urllib import error

import requests
from bs4 import BeautifulSoup
from joblib import Parallel, delayed
from rotten_tomatoes_scraper.rt_scraper import MovieScraper

cant_find_data = {'Score_Rotten': '-1',
                  'Score_Audience': '-1',
                  'Rating': 'Unknown',
                  'Genre': ['Unknown']}


def get_kino_movies():
    url = 'http://kino.dk'
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    movie_count = soup.find_all(
        'span', {'class': 'carousel-movies-count'})[0]
    movie_count = int(movie_count.get_text().split(' ')[0])

    movies = set()
    elements = soup.find_all('a')
    for element in elements:
        href = element.attrs.get('href')
        if href and href.split('/')[1] == 'film':
            if element.get_text() != '':
                movies.add(element.get_text())
                if len(movies) == movie_count:
                    break

    return movies, movie_count


def search_rotten_tomatoes(movie):
    try:
        movie_scraper = MovieScraper(movie_title=movie)
        movie_scraper.extract_metadata()
        if not movie_scraper.metadata.get('Score_Rotten'):
            movie_scraper.metadata['Score_Rotten'] = '-1'
        if not movie_scraper.metadata.get('Score_Audience'):
            movie_scraper.metadata['Score_Audience'] = '-1'
        return (movie, movie_scraper.metadata, True)

    # Can't find movie from Kino name
    # There's tons of cleaning that can be done, to help find the movie
    # But can't be bothered right now
    except (AttributeError, IndexError, error.HTTPError):
        return (movie, cant_find_data, False)


def sort_and_print(movie_scores, title, key, top=20):
    sorted_movies = sorted(movie_scores, key=key, reverse=True)
    print(title)
    for movie in sorted_movies[:20]:
        print(movie[0], movie[1])
    print()


def main():
    movies, total_movies = get_kino_movies()

    parallel = Parallel(n_jobs=-1)
    movie_scores = parallel(delayed(search_rotten_tomatoes)(movie)
                            for movie in movies)

    sort_and_print(movie_scores, 'Top 20 Rotten Scores',
                   key=lambda x: int(x[1]['Score_Rotten']))

    sort_and_print(movie_scores, 'Top 20 Audience Scores',
                   key=lambda x: int(x[1]['Score_Audience']))

    count = 0
    for movie in movie_scores:
        if movie[2]:
            count += 1

    print(f"Found {count}/{total_movies} movies")


if __name__ == '__main__':
    main()
