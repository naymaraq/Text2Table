def prettify(json):
    pretty_json = []
    rel_triples = []
    for item in json:
        item_rels = {"Text": " ".join(item["tokens"]),
                     "DetectedRelations": []
                    }
        triple = []
        for realtion in item["relations"]:
            head_ix = realtion["head"]
            tail_ix = realtion["tail"]
            rel_type = realtion["type"]

            head = item["entities"][head_ix]
            tail = item["entities"][tail_ix]

            head_span = ' '.join(item["tokens"][head["start"]:head["end"]])
            tail_span = ' '.join(item["tokens"][tail["start"]:tail["end"]])

            item_rels["DetectedRelations"].append("{rel_type}({head_span},{tail_span})".format(rel_type=rel_type,
                                                                     head_span=head_span,
                                                                     tail_span=tail_span))
            triple.append((rel_type,head_span,tail_span))
        pretty_json.append(item_rels)
        rel_triples.append(triple)
    return pretty_json, rel_triples
