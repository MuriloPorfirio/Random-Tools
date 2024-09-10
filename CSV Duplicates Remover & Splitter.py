#This script was designed to be compatible with VSCode.
#If you encounter any errors while running it, make sure all the necessary libraries are installed.

#Now that removing duplicates in Rayyan is a paid feature, they only allow you to remove duplicates one by one,
#considering similarities of up to 50%. Unless you pay for the premium version, you can't change this setting,
#resulting in thousands of duplicates that need to be manually removed. An alternative is to use this script.
#To use it, you first need to have the CSV file of all the articles imported into Rayyan.
#You can obtain this file by downloading it directly from Rayyan, even if you imported your libraries in multiple parts at different times.
#Rayyan allows you to download the entire dataset as a CSV file.

#The script will remove duplicates, but not based on a similarity percentage, as this would require a lot of processing and take too much time.
#Instead, it uses strategies based on identical regions in the title and abstract.
#This significantly helps in identifying duplicates, leaving only a small number for you to remove manually.

#Additionally, the script splits the output into files of up to 90MB, since Rayyan only allows uploads of files up to 100MB.
#For instance, if you have 200MB of data, the script will return three files, each with a maximum size of 90MB.

import pandas as pd
import os
from tkinter import Tk
from tkinter.filedialog import askopenfilenames
from tqdm import tqdm  # Biblioteca para exibir a barra de progresso

# Função para remover duplicatas exatas
def remove_exact_duplicates(df, column_name):
    return df.drop_duplicates(subset=[column_name])

# Função para comparar títulos removendo uma palavra específica (primeira, última, etc.)
def remove_word_and_compare(df, column_name, word_position):
    seen_titles = set()
    unique_rows = []
    
    for index, row in tqdm(df.iterrows(), total=len(df), desc="Processando Remoção de Palavras"):
        title = str(row[column_name]).strip().split()
        if len(title) > word_position:
            if word_position == 0:  # Remove primeira palavra
                modified_title = " ".join(title[1:])
            elif word_position == -1:  # Remove última palavra
                modified_title = " ".join(title[:-1])
            elif word_position == 1:  # Remove segunda palavra
                modified_title = title[0] + " " + " ".join(title[2:])
            else:  # Remove palavra do meio
                middle = len(title) // 2
                modified_title = " ".join(title[:middle] + title[middle+1:])
            
            if modified_title not in seen_titles:
                seen_titles.add(modified_title)
                unique_rows.append(row)
    
    return pd.DataFrame(unique_rows)

# Função para remover duplicatas por abstract
def remove_abstract_duplicates(df, abstract_column):
    return df.drop_duplicates(subset=[abstract_column])

# Função para dividir o arquivo em partes de até 90 MB de maneira eficiente
def split_csv_file(df, base_filename, max_size_mb=90):
    max_size_bytes = max_size_mb * 1024 * 1024
    part_number = 1
    current_size = 0
    chunk = []
    
    # Percorre o DataFrame e vai salvando em partes
    for index, row in tqdm(df.iterrows(), total=len(df), desc="Dividindo Arquivos"):
        chunk.append(row)
        current_size += row.to_csv(index=False).encode('utf-8').__sizeof__()  # Calcula o tamanho incremental
        
        if current_size > max_size_bytes:
            chunk_df = pd.DataFrame(chunk)
            chunk_filename = f"{base_filename}_parte_{part_number}.csv"
            chunk_df.to_csv(chunk_filename, index=False)
            print(f"Salvando {chunk_filename}...")
            chunk = []  # Reiniciar o chunk
            part_number += 1
            current_size = 0
    
    # Salva o último chunk, se houver
    if chunk:
        chunk_df = pd.DataFrame(chunk)
        chunk_filename = f"{base_filename}_parte_{part_number}.csv"
        chunk_df.to_csv(chunk_filename, index=False)
        print(f"Salvando {chunk_filename}...")

# Função para abrir a janela de seleção de arquivos
def select_files():
    root = Tk()
    root.withdraw()  # Ocultar a janela principal
    file_paths = askopenfilenames(filetypes=[("CSV files", "*.csv")])  # Permite selecionar múltiplos arquivos CSV
    return file_paths

# Função para processar os arquivos CSV
def process_files(file_paths):
    # Carregar os arquivos CSV em DataFrames e concatenar com low_memory=False e chunksize
    dfs = []
    for file in file_paths:
        print(f"Processando {file}...")
        chunks = pd.read_csv(file, low_memory=False, chunksize=5000)  # Lendo em blocos menores de 5000 linhas
        for chunk in chunks:
            dfs.append(chunk)
    
    df_combined = pd.concat(dfs, ignore_index=True)

    # Exibir os nomes das colunas para verificar o nome correto
    print("Colunas disponíveis:", df_combined.columns)

    # Substitua 'Title' por 'title'
    df_no_exact_duplicates = remove_exact_duplicates(df_combined, column_name='title')

    # Etapa 2: Comparação por remoção de palavras
    df_no_first_word = remove_word_and_compare(df_no_exact_duplicates, column_name='title', word_position=0)
    df_no_last_word = remove_word_and_compare(df_no_first_word, column_name='title', word_position=-1)
    df_no_second_word = remove_word_and_compare(df_no_last_word, column_name='title', word_position=1)
    df_no_middle_word = remove_word_and_compare(df_no_second_word, column_name='title', word_position=2)

    # Etapa 3: Remover duplicatas por abstract, se houver uma coluna de abstracts
    if 'abstract' in df_combined.columns:
        df_final = remove_abstract_duplicates(df_no_middle_word, abstract_column='abstract')
    else:
        df_final = df_no_middle_word

    # Dividir o arquivo em partes de no máximo 90 MB de maneira eficiente
    base_filename = "artigos_sem_duplicados_processados"
    split_csv_file(df_final, base_filename=base_filename, max_size_mb=90)

# Execução do script com janela de seleção de arquivos e barra de progresso
if __name__ == "__main__":
    print("Selecione os arquivos CSV.")
    file_paths = select_files()

    if file_paths:
        process_files(file_paths)
    else:
        print("Nenhum arquivo selecionado.")
