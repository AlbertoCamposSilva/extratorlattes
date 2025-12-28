from flask import Flask, request, send_file
import extratorlattes as lt
import datetime

app = Flask(__name__)

@app.route("/upload/<lattes_id>", methods=["POST"])
def upload(lattes_id):
    if request.method == "POST":
        file = request.files["file"].read()
        lattes=lt.Lattes(lattes_id)
        lattes.zip = file
        if lattes.get_xml():
            lattes.save_zip_to_disk()
            with open (r'C:\Lattes_Excrator\log.txt', 'a') as file:
                file.write(f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} - Importado o id {lattes.id}.\n')
        else:
            with open (r'C:\Lattes_Excrator\log.txt', 'a') as file:
                file.write(f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")} - Erro ao importar o id {lattes_id}.\n')
        return "Lattes recebido."
    
@app.route("/download/<lattes_id>")
def download(lattes_id):
    lattes = lt.Lattes(lattes_id)
    lattes.read_zip_from_disk()
    return send_file(lattes.zip, as_attachment=True)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8182)