import json


def get_json(filepath, number):
    with open(filepath) as infile:
        tops = json.load(infile)
        tops = next(top for top in tops if top['session'] == '18/{}'.format(number))

    tops = [top for top in tops['tops'] if len(top['speakers']) > 1]

    presidents = ["Lammert, Prof. Dr. Norbert",
                  "Noll, Michaela",
                  "Bulmahn, Dr. h. c. Edelgard",
                  "Bartsch, Dr. Dietmar",
                  "Pau, Petra",
                  "Schmidt (Aachen), Ulla",
                  "Riesenhuber, Prof. Dr. Heinz",
                  "Roth (Augsburg), Claudia",
                  "Singhammer, Johannes",
                  "Hintze, Peter"]

    all_speakers = []
    for top in tops:
        all_speakers += [{'speaker': s, 'top': top['topic'], 'top_obj': top} for s in top['speakers'] if s not in presidents]

    new = []
    for s in all_speakers:
        last, first = s['speaker'].split(", ")
        s['speaker'] = "{} {}".format(first, last)
        if not new or new[-1]['speaker'] != s['speaker']:
            new.append(s)
    return new