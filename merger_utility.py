from itertools import groupby
from collections import defaultdict
from datetime import datetime
import re
import locale

import json


def json_top_merge(session_speaker, session_detail, session_classes):
    new_sessions = []
    for session in session_speaker:

        session_name = re.sub('^[0-9]*\/', '', session['session'])
        print("merging tops for session: " + session_name)
        new_tops = []
        for top in session['tops']:
            name = top['topic'].replace('\n', '')
            detail_top_list = [x for x in session_detail if name == 'TOP ' + x['number'] + ' ' + x['title']]

            categories = [session_class['categories_cleaned'] for session_class in session_classes
                              if session_class['session'] == session_name and
                            str(session_class['title']) in name]
            if len(categories) == 0 and 'Sitzungseröffnung' not in name and 'Sitzungsende' not in name:
                # print("Error for session " + session_name + " and TOP " + name)
                print(session_name + ";" + repr(name) + ";" + session['date'])
            else:
                # print("Success for session " + session_name + " and TOP " + name)
                i = 0

            detail = detail_top_list[0] if len(detail_top_list) > 0 else None
            category = categories[0] if len(categories) > 0 else None

            if not category:
                print("Category is empty for session " + session_name + " and TOP " + name)

            top['categories'] = category
            if (detail):
                top['description'] = detail['description']
                top['number'] = detail['number']
                top['title_clean'] = detail['title']
                top['detail'] = detail['detail']
                top['year'] = detail['year']
                top['week'] = detail['week']
                top['duration'] = detail['duration']
            new_tops.append(top)
        session['tops'] = new_tops
        new_sessions.append(session)
    return new_sessions


def get_json_file(filename):
    with open(filename) as data:
        return json.load(data)


def write_json_file(filename, data):
    with open(filename, 'w') as outfile:
        json.dump(data, outfile)


def simplify_classes(filepath):
    with open(filepath) as infile:
        answers = json.load(infile)
        cleaned_entries = []

        # building groups of answers for the same TOP
        groupings = defaultdict(list)
        for obj in answers:
            key = obj['info']['session'] + '-' + obj['info']['number']
            groupings[key].append(obj)
        groups = groupings.values()

        # check if entries have the same shape, if not take the first classification
        for group in groups:
            categories = [item['info']['categories'] for item in group ]
            length = len(categories[0])
            if any(len(lst) != length for lst in categories):
                print('Error: not the same length for session:' + group[0]['info']['session'] + ', TOP:' + group[0]['info']['number'] + '')
            else:
                zipped = zip(*categories)
                for cat_list in zipped:
                    if not all(x == cat_list[0] for x in cat_list):
                        print("Error: different categories for session:" + group[0]['info']['session'] + ', TOP: ' + group[0]['info']['number'])

            cleaned_entries.append(group[0]['info'])
        return cleaned_entries