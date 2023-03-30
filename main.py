import glob
import os
from datetime import datetime
from calcolo_cycle_time import *

# posizione delle cartelle con tutti gli artcode
datapath = "C:\\Users\\stefano\\Desktop\\Dinema\\ARTCODE\\clientftp"
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start = datetime.now()
    os.chdir(datapath)
    df_completo = pd.DataFrame()
    sub_res = []
    hash_set = set()
    dict_programmi_cycle_time = {}
    counter_multiple_hash = 0
    # salva in directories solo le directory presenti in datapath
    directories = [d for d in os.listdir() if os.path.isdir(d)]
    for directory in directories:
        start3 = datetime.now()
        machine_id = directory
        files = glob.glob(os.path.join(directory, '*.cot'))
        for filename in files:
            absolute_filename_path = datapath + "\\" + filename
            start2 = datetime.now()
            # file è fatto cosi: 2023.03.06082654.624035_C134-XS_tg1.cot
            hash = get_splitted_artcode_hash(absolute_filename_path)
            if hash in hash_set:
                counter_multiple_hash += 1
                end2 = datetime.now()
                print("hash esistente: {}".format(end2 - start2))
                continue
            else:
                hash_set.add(hash)
                # il timestamp è la parte prima del primo underscore
                timestamp = filename.split("_")[0].split("\\")[
                    1]  # perchè il filename è composto da direcotry//filename
                # program_name è tutta la parte dopo il primo underscore
                # (prendo gli split dopo il primo underscore e rimetto l'underscore in mezzo)
                program_name = '_'.join(filename.split('_')[1:])
                # creo un df con le colonne: step,course,rpm,econ,step_repeated,nome_zona,codice_zona
                df = crea_dataframe_rango_unico(absolute_filename_path)
                # aggiungo al df le colonne con le informazioni recuperate dai nomi delle directory/file
                df["timestamp"] = timestamp
                df["machine_id"] = machine_id
                df["program"] = program_name
                df["hash"] = hash
                sub_res.append(df)
                # creo un dizionario con chiave il nome del programma e chiave il cycle time
                # df_cycle = crea_dataframe_cycle_time_tagliato(filename)
                # cycle_time = calcolo_cycle_time(df_cycle, filename)
                # dict_programmi_cycle_time[filename]=cycle_time
            end2 = datetime.now()
            print("sub df creato in: {}".format(end2 - start2))
        end3 = datetime.now()
        print("directory completata in: {}".format(end3 - start3))
        print("__________")
    df_completo = pd.concat(sub_res, ignore_index=True)
    end = datetime.now()
    print(df_completo)
    print(df_completo.shape)
    print("df completo creato in: {}".format(end - start))
    print("hash multipli trovati: {}".format(counter_multiple_hash))
    print("lunghezza hash_list: {}".format(len(hash_set)))
    # calcolo del num di righe duplicate considerando hash e course (contrllo che course sia unico)
    num_righe_duplicate = df_completo.duplicated(subset=['hash', 'course']).sum()
    print(f"Il numero di righe duplicate considerando le colonne hash e course è: {num_righe_duplicate}.")
    print("_____________")
    for item, key in dict_programmi_cycle_time.items():
        print(f"Il programma {item} ha tempo di ciclo teorico di {key}s")
