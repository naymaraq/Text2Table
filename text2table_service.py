from flask import Flask, request, jsonify
from one_shot_infer import SpertClinet, process_config

app = Flask(__name__)

config_path = "configs/example_eval.conf"
run_args, run_config = process_config(config_path)
spert_clinet = SpertClinet(run_args)

def prettify(json):
    pretty_json = []
    for item in json:
        item_rels = {"Text": " ".join(item["tokens"]),
                     "DetectedRelations": []
                    }
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
        pretty_json.append(item_rels)
    return pretty_json


@app.route("/")
def home():
    text = request.args.get('text')
    result = spert_clinet(text)
    return jsonify(prettify(result))


if __name__ == "__main__":
    app.run(host="localhost", port=6969)
