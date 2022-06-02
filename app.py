from threading import Thread

from flask import Flask, flash, request, redirect, jsonify
from comprehend_clasifier import ComprehendDetect, LanguageEnum

ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

app = Flask(__name__)

comprehend_classifier = ComprehendDetect()


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def detect(file_content):
    languages = comprehend_classifier.detect_languages(file_content)
    lang_code = languages[0]['LanguageCode']

    functions = [
        comprehend_classifier.detect_entities,
        comprehend_classifier.detect_key_phrases,
        comprehend_classifier.detect_sentiment,
        comprehend_classifier.detect_syntax,
    ]
    demo_size = 5
    results = []
    for i in functions:
        language = getattr(LanguageEnum, lang_code).value
        thread = Thread(
            target=i,
            args=(file_content, language, results, demo_size)
        )
        thread.start()
        thread.join()
    return results


@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        file_content = file.read().decode('utf-8')

        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):

            results = detect(file_content)

            return jsonify(results)
    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=file>
      <input type=submit value=Upload>
    </form>
    '''


if __name__ == '__main__':
    app.run(debug=True)
