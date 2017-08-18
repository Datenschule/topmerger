import logging

import click
import locale
from datetime import datetime
from normdatei.text import fingerprint, clean_name
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session, relationship
from tqdm import tqdm
from Levenshtein import distance

from jsons import get_json
from merger_utility import *


@click.command()
@click.option('--db_url', required=True, default='postgres://postgres@0.0.0.0:32780')
@click.option('--tops_path', type=click.Path(exists=True), required=True)
@click.option('--session_path', type=click.Path(exists=True), required=True)
@click.option('--classes_path', type=click.Path(exists=True), required=True)
@click.option('--verbose', is_flag=True)
@click.option('--start', type=click.INT, default=0)
@click.option('--end', type=click.INT, default=245)
def main(db_url, tops_path, session_path, classes_path, verbose, start, end):
    """Merge Utterances from a db with topics from a json file"""
    cleaned_classes = simplify_classes(classes_path)
    speaker = get_json_file(tops_path)
    detail = get_json_file(session_path)
    merged_tops = json_top_merge(speaker, detail, cleaned_classes)
    write_json_file('data/merged.json', merged_tops)
    if verbose:
        logging.basicConfig(level=logging.INFO)
    init_sqlalchemy(dbname=db_url)
    for i in tqdm(range(start, end)):
        try:
            # run_for(i, tops_path)
            run_for(i, 'data/merged.json')
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
    type = Column(String)
    text = Column(String)
    top_id = Column('top_id', Integer, ForeignKey("tops.id"), nullable=True)

    @staticmethod
    def get_all(wahlperiode, sitzung, session):
        return session.query(Utterance) \
            .filter(Utterance.sitzung == sitzung) \
            .filter(Utterance.wahlperiode == wahlperiode) \
            .order_by(Utterance.sequence) \
            .all()


class Top(Base):
    __tablename__ = "tops"
    id = Column(Integer, primary_key=True)
    wahlperiode = Column(Integer)
    sitzung = Column(Integer)
    title = Column(String)
    title_clean = Column(String)
    description = Column(String)
    number = Column(String)
    week = Column(Integer)
    detail = Column(String)
    year = Column(Integer)
    category = Column(String)
    duration = Column(Integer)
    held_on = Column(Date)

    def save(self):
        try:
            DBSession.add(self)
            DBSession.commit()
        except Exception as se:
            print(se)
            DBSession.rollback()

    @staticmethod
    def delete_for_session(wahlperiode, sitzung):
        DBSession.query(Top) \
            .filter_by(wahlperiode=wahlperiode) \
            .filter_by(sitzung=sitzung) \
            .delete()


def init_sqlalchemy(dbname):
    global engine
    engine = create_engine(dbname, echo=False)
    DBSession.remove()
    DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)


def fingerclean(name):
    # TODO: Replace this string in Normdatei?
    name = name.replace("Frhr.", "Freiherr")
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
        cleaned_top_speaker = fingerclean(entry['speaker'])
        cleaned_protocol_speaker = fingerclean(plpr[index + offset].speaker_cleaned)
        while distance(cleaned_protocol_speaker, cleaned_top_speaker) > 3:
            logging.info('Comparing: %s ... %s', entry['speaker'], plpr[index + offset].speaker_cleaned)
            offset += 1
            cleaned_top_speaker = fingerclean(entry['speaker'])
            cleaned_protocol_speaker = fingerclean(plpr[index + offset].speaker_cleaned)
        logging.info('Match: %s -> %s', entry['speaker'], plpr[index + offset].speaker_cleaned)
        if not results or results[-1]['topic'] != entry['top']:
            results.append({'sequence': plpr[index + offset].sequence, 'topic': entry['top'], 'top_obj': entry['top_obj']})

    update_utterances(utterances, results, SESSION)

    DBSession.bulk_save_objects(utterances)
    DBSession.commit()


def update_utterances(utterances, results, session):
    last_utterance = 0
    Top.delete_for_session(18, session)
    for index in range(len(results)):
        current_top = results[index]
        try:
            next_top = results[index + 1]
        except IndexError:
            next_top = {'sequence': 100000000}
        top_obj = current_top['top_obj']
        loc = locale.setlocale(locale.LC_TIME,("de_DE"))
        date = None
        if 'date' in top_obj.keys() and len(top_obj['date']) > 0:
            date = datetime.strptime( top_obj['date'], "%d. %B %Y")
        top = Top(wahlperiode=18,
                  sitzung=session,
                  title=current_top['topic'],
                  category=";".join(top_obj['categories']),
                  description=top_obj['description'] if 'description' in top_obj.keys() else None,
                  detail=top_obj['detail'] if 'detail' in top_obj.keys() else None,
                  number=top_obj['number'] if 'number' in top_obj.keys() else None,
                  title_clean=top_obj['title_clean'] if 'title_clean' in top_obj.keys() else None,
                  week=top_obj['week'] if 'week' in top_obj.keys() else None,
                  year=top_obj['year'] if 'year' in top_obj.keys() else None,
                  duration=top_obj['duration'] if 'duration' in top_obj.keys() else None,
                  held_on=date
                  )
        top.save()
        for u in utterances[last_utterance:]:
            if u.sequence < next_top['sequence']:
                u.top_id = top.id
            else:
                last_utterance = u.sequence
                break


if __name__ == "__main__":
    main()
