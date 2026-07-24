import json
import time
import tkinter as tk
from tkinter import messagebox
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

def start_tracking():
    print("Iniciando o Chrome com logs de performance ativados...")
    chrome_options = Options()
    # Ativa os logs de performance (Network)
    chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    
    driver.get("http://buscacv.cnpq.br/buscacv/")
    
    # Prepara o tkinter para mostrar um botão de concluir
    root = tk.Tk()
    root.title("Monitoramento Lattes")
    root.geometry("300x150")
    
    lbl = tk.Label(root, text="Faça o login e o download do CV\nno navegador aberto.\nQuando concluir, clique abaixo:", wraplength=280)
    lbl.pack(pady=10)
    
    def on_concluir():
        print("Coletando logs de rede...")
        try:
            logs = driver.get_log("performance")
            
            with open("logs_rede.json", "w", encoding="utf-8") as f:
                json.dump(logs, f, indent=4, ensure_ascii=False)
                
            print("Logs salvos com sucesso em 'logs_rede.json'!")
            messagebox.showinfo("Sucesso", "Logs salvos! Pode fechar esta janela.")
        except Exception as e:
            print(f"Erro ao capturar logs: {e}")
            messagebox.showerror("Erro", f"Ocorreu um erro: {e}")
        finally:
            driver.quit()
            root.destroy()
            
    btn = tk.Button(root, text="Concluir e Salvar Logs", command=on_concluir, bg="green", fg="white", font=("Arial", 12))
    btn.pack(pady=10)
    
    # Mantém o tkinter rodando no topo
    root.attributes("-topmost", True)
    root.mainloop()

if __name__ == "__main__":
    start_tracking()
