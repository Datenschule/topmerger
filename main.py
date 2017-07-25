import logging

import click
from normdatei.text import fingerprint, clean_name
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from tqdm import tqdm

from jsons import get_json


@click.command()
@click.option('--db_url', required=True, default='postgres://postgres@0.0.0.0:32780')
@click.option('--tops_path', type=click.Path(exists=True), required=True)
@click.option('--verbose', is_flag=True)
@click.option('--start', type=click.INT, default=0)
@click.option('--end', type=click.INT, default=245)
def main(db_url, tops_path, verbose, start, end):
    """Merge Utterances from a db with topics from a json file"""
    if verbose:
        logging.basicConfig(level=logging.INFO)

    init_sqlalchemy(dbname=db_url)
    for i in tqdm(range(start, end)):
        try:
            run_for(i, tops_path)
        except Exception as e:
            print("Failed with {} for {}".format(e, i))


Base = declarative_base()
DBSession = scoped_session(sessionmaker())
engine = None


class Utterance(Base):
    __tablename__ = "de_bundestag_plpr"

    id = Column(Integer, primary_key=True)
    wahlperiode = Column(Integer)
    sitzung = Column(Integer)
    sequence = Column(Integer)
    speaker_cleaned = Column(String)
    speaker_party = Column(String)
    top = Column(String)
    type = Column(String)
    text = Column(String)

    @staticmethod
    def get_all(wahlperiode, sitzung, session):
        return session.query(Utterance) \
            .filter(Utterance.sitzung == sitzung) \
            .filter(Utterance.wahlperiode == wahlperiode) \
            .order_by(Utterance.sequence) \
            .all()


def init_sqlalchemy(dbname):
    global engine
    engine = create_engine(dbname, echo=False)
    DBSession.remove()
    DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)


def fingerclean(name):
    return fingerprint(clean_name(name))


def get_speaker_sequence(utterances):
    results = []
    for u in utterances:
        if u.type == 'speech':
            if not results or u.speaker_cleaned != results[-1].speaker_cleaned:
                results.append(u)
    return results


def run_for(SESSION, tops_path):
    utterances = Utterance.get_all(18, SESSION, DBSession)

    plpr = get_speaker_sequence(utterances)

    json_data = get_json(tops_path, SESSION)

    results = []
    offset = 0
    for index, entry in enumerate(json_data):
        logging.info('%s -> %s', entry['speaker'], plpr[index + offset].speaker_cleaned)
        while fingerclean(entry['speaker']) != fingerclean(plpr[index + offset].speaker_cleaned):
            logging.info('%s ... %s', entry['speaker'], plpr[index + offset].speaker_cleaned)
            offset += 1
        if not results or results[-1]['topic'] != entry['top']:
            results.append({'sequence': plpr[index + offset].sequence, 'topic': entry['top']})

    update_utterances(utterances, results)

    DBSession.bulk_save_objects(utterances)
    DBSession.commit()


def update_utterances(utterances, results):
    last_utterance = 0
    for index in range(len(results)):
        current_top = results[index]
        try:
            next_top = results[index + 1]
        except IndexError:
            next_top = {'sequence': 100000000}
        for u in utterances[last_utterance:]:
            if u.sequence < next_top['sequence']:
                u.top = current_top['topic']
            else:
                last_utterance = u.sequence
                break


if __name__ == "__main__":
    main()
