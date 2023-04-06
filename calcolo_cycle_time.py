import pandas as pd
import re
import numpy as np
from crea_df import *

#N.B. per calcolare il cycle time creare prima il df con crea_dataframe_cycle_time_tagliato(filename), poi richiama calcolo_cycle_time

#<--dict: dizionario dei sottraendi creato nella funzione "rimuovi zone di gestione doppia"
#-->dict: dizionario con i valori aggiornati
#===il dizionario originale contiene la lunghezza in termini di posLin delle zone rimosse
#===per compattare la posLin il dizionario originale non va bene in quanto nei punti dopo la seconda zona rimossa devo
#===sottrarre la lunghezza della seconda zona rimossa più la lunghezza della prima. perciò utilizzo questa funzione che calcola il sottraendo corretto
def somma_dict_sottraendi(dict):
  # ad ogni valore del dict devo sommare il valore del precedente item del dizionario
  somma = 0
  for k, v in dict.items():
    somma += v
    dict[k] = somma
  return dict


#<--df: dataframe con zone di gestione doppia già rimosse
#<--dict_sottraendi: dizionario che riporta quanto sottrarre per compattare la posLin
#<--zone_gestione_doppia: la lista delle zone di gestione doppia. ogni elemento è composto dalla coppia inizio-fine zona
#-->df: dataframe con la posLin compattata
#===rimuove i salti tra posLin dovuti alla rimozione di zone di gestione doppia. prima delle prima zona della lista non faccio nulla.
#===tra la prima e la seconda sottrai il primo sottraendo del dict, dopo la seconda sottrai il secondo sottraendo del dict e cosi via
def compatta_posLin(df,dict_sottraendi,zone_gestione_doppia):
  if len(dict_sottraendi)!=len(zone_gestione_doppia):
    raise Exception("il numero di elementi della lista dei sottraendi è diverso dal numero di elementi della lista delle zone di gestione doppia")
  for i, (chiave, valore) in enumerate(dict_sottraendi.items()):
    if i < len(dict_sottraendi) - 1:
      chiave_successiva, valore_successivo = list(dict_sottraendi.items())[i+1]
      #sottraggo a posLin delle righe con posLin tra chiave(compreso) e chiave_successiva(non compreso) il valore corrente
      filtro = (df['posLin'] >= chiave) & (df['posLin'] < chiave_successiva)
      df.loc[filtro, 'posLin'] -= valore
    else:#ultimo elemento del dict. processa fino a fine df
      #sottraggo a posLin delle righe con posLin tra chiave(compreso) e l'ultimo elemento del df il valore corrente
      filtro = (df['posLin'] >= chiave) & (df['posLin'] <= df["posLin"][df.index[-1]])
      df.loc[filtro, 'posLin'] -= valore
  return df

#-->df: df a cui rimuovere le zone di gestione doppia
#<--df: df con zone di gestion doppia rimosse e poslin compattata
#===funzione che rimuove le zone di gestione doppia e compatta la posLin
def rimuovi_zone_gestione_doppia_e_compatta(df):
  lista_zone_gestione_doppia=trova_zone_gestione_doppia(df)
  #lista_zone_gestione_doppia contiene una serie di elementi composti da una coppia (inizio,fine)
  dict_sottraendi={}
  #per ogni zona di gestione doppia calcolo il sottraendo (differenza tra posLin della riga precedente alla fine zona di gestione doppia e posLin della riga precedente all'inizio zona di gestione doppia)
  #e creo un dizionario con chiave l'upper bound della zona di gestione doppia e valore la differenza trovata precedentemente
  for x in lista_zone_gestione_doppia:
    sottraendo = x[1] - x[0]
    dict_sottraendi[x[1]] = sottraendo
    df = df.drop(df.index[(df["posLin"] >= x[0]) & (df["posLin"] < x[1])])
  dict_sottraendi=somma_dict_sottraendi(dict_sottraendi)
  #chiamo la funzione che compatta le posLin in base al dizionario appena creato
  compatta_posLin(df,dict_sottraendi,lista_zone_gestione_doppia)
  #df = df.astype({"posLin": int, "step": int,"rango": int,"rpm": int,"economia":bool,"codice_zona":int,"nome_zona":str,"step_ripetuto":bool})
  df = df.reset_index(drop=True)
  return df

# conversione da rpm a secondi per giro considerando l'accelerazione (velocità media tra punto iniziale e finale)
def tempo_giro_acc(v_i, v_f):
    secondi_giro = 1 / (((v_i + v_f) / 2) / 60)
    return secondi_giro


# conversione da rpm a secondi per giro senza considerare l'accelerazione
def tempo_giro_no_acc(rpm):
    secondi_giro = 1 / (rpm / 60)
    return secondi_giro


# funzione che dato in ingresso la velocità iniziale, quella finale, la rampa e il numero di aghi ritorna
# il numero di giri (espresso sotto forma di poslin) con i quali si raggiunge la velocità finale
def calcola_poslin(v_i, v_f, acc, aghi):
    t = (int(v_f) - int(v_i)) / int(acc)
    v_m = (int(v_f) + int(v_i)) / 2
    rps = v_m / 60
    n_giri = rps * t
    variazione_poslin = n_giri * aghi
    return variazione_poslin


# funzione che calcola incrementalmente il cycle time in secondi.
def calcolo_cycle_time(df, filename):
    n_aghi = leggi_n_aghi(filename)
    cycle_time = float(0)  # variabile che conterrà il tempo complessivo
    for i, row in df.iterrows():
        # per la prima riga del df pongo cycle_time=0
        if i == 0:
            cycle_time += 0.0
        else:
            if i == 1:  # dato che la macchina parte da ferma considero la vecolità media tra 0 e la velcoità letta nella prima riga del df
                previous_poslin = float(df.loc[i - 1, "posLin"])
                previous_rpm = float(df.loc[i - 1, "rpm_azionam"])
                actual_poslin = float(row["posLin"])
                cycle_time += tempo_giro_acc(0, previous_rpm) * ((actual_poslin - previous_poslin) / n_aghi)
            else:  # incremento il cycle time senza considerare l'accelerazione
                previous_poslin = float(df.loc[i - 1, "posLin"])
                previous_rpm = float(df.loc[i - 1, "rpm_azionam"])
                actual_poslin = float(row["posLin"])
                cycle_time += tempo_giro_no_acc(previous_rpm) * ((actual_poslin - previous_poslin) / n_aghi)
    return cycle_time


# cerca nell'artcode l'informazione relativa al numero di aghi. contenuta dopo la stringa "n_aghi_pr_c". (ci sono più stringhe uguali)
def leggi_n_aghi(filename):
    pattern_aghi = re.compile(r"^n_aghi_pr_c")
    trovato = False
    with open(filename, 'r') as f:
        for line in f:
            if trovato:#uso per bloccare la lettura al primo "n_aghi_pr_c" individuato
                return aghi
            if re.search(pattern_aghi, line.strip()):
                aghi = int(line.split("=")[1])
                trovato = True


# crea un df contenente poslin e rpm_azionam (sono contenute le informazioni lette dagli azionamenti)
def crea_df_vel(filename):
    pattern_vel = re.compile(r"^AZIONAM_VEL_POSLIN /")
    pattern_rampa = re.compile(r"^AZIONAM_VEL_POSLIN_RAMPA")
    pattern_start_rampa = re.compile(r"^AZIONAM_START_EXEC_RAMPA")
    df_rows = []
    aghi = leggi_n_aghi(filename)
    vel_riga_prec = 0
    with open(filename, 'r') as f:
        for line in f:
            # se la riga è del tipo "^AZIONAM_VEL_POSLIN /", allora contiene le informazioni sulle velcotà giuste
            if re.search(pattern_vel, line.strip()):
                poslin = int(line.split("/")[1].split("=")[1])
                vel = int(line.split("/")[2].split("=")[1])
                vel_riga_prec = vel
                df_rows.append([poslin, vel])
            # se la riga è del tipo "^AZIONAM_VEL_POSLIN_RAMPA" allora contiene le info sulla rampa
            #perciò deve calcoalre quando verrà raggiunta la velocità specificata
            if re.search(pattern_rampa, line.strip()):
                start_rampa_trovato = False
                while start_rampa_trovato == False:
                    next_line = next(f, None)
                    # nella riga successiva (alcune volte non è quella sucessiva ma la seconda successiva)
                    # è riportata la poslin di inizio rampa.
                    # mentre nella riga corrente trovo le info di vel_iniziale, vel_finale e rampa
                    if re.search(pattern_start_rampa, next_line.strip()):
                        start_rampa_trovato = True
                        poslin = int(next_line.split("/")[1].split("=")[1])
                        vel = int(vel_riga_prec)
                        df_rows.append([poslin, vel])
                        rampa = int(line.split("/")[2].split("rampa=")[1].split(" ")[0])
                        vel_finale = int(line.split("/")[2].split("vel=")[1].split(" ")[0])
                        # calcolo a quale posizione lineare raggiungo la velcoità finale
                        poslin_calc = abs(calcola_poslin(vel, vel_finale, rampa, aghi))
                        df_rows.append([poslin + poslin_calc, vel_finale])
                        vel_riga_prec = vel_finale
    df_cycle = pd.DataFrame(df_rows, columns=["posLin", "rpm_azionam"])
    df_cycle = df_cycle.astype({"posLin": int, "rpm_azionam": int})
    return df_cycle


# Funzione che ritorna un df NON a rango unico ottentuo sfruttando art_forstep e azionamenti.
# rpm_azionam è ottimizzata per calcolare il cycle time
def crea_dataframe_cycle_time_tagliato(filename):
    df_step = crea_sub_df("step_actual", "step", filename)
    df_rango = crea_sub_df("rango_actual", "rango", filename)
    df_rpm = crea_sub_df("rpm_program", "rpm", filename)
    df_art_forstep = crea_sub_df("ART_FORSTEP_ECONOM_ACTUAL", "forstep_econ",filename)
    df = pd.DataFrame()
    df = df_step.merge(df_rango, how='outer', on="posLin", sort=True)
    # modifica df_rpm in modo da essere compatibile con step e rango e merge
    df_rpm_modificato = make_df_compatible_with_step(df_rpm, df)
    df = df.merge(df_rpm_modificato, how='outer', on="posLin", sort=True)
    # merge con df_forstep
    df = df.merge(df_art_forstep, how='outer', on="posLin", sort=True)
    #creo df_vel leggendo la parte degli azionamenti
    df_vel = crea_df_vel(filename)
    df = df.merge(df_vel, how='outer', on="posLin", sort=True)
    # gestion NaN
    df = df.fillna(method='ffill')  # ForwardFILL
    df = df.fillna(method='bfill')  # BackwardFILL
    # aggiungo al df la colonna codice_zona che è rappresentata dalla lista_codici appena creata
    # gestione delle righe con posLin duplicata: mantengo il primo elemento
    for valore_posLin in df['posLin'].unique():
        righe_duplicate = df[df['posLin'] == valore_posLin].duplicated(subset='posLin')
        if any(righe_duplicate):
            df = df.drop(df[df['posLin'] == valore_posLin][righe_duplicate].index[1])
    # assegno ad ogni colonna il tipo desiderato
    df = df.astype({"posLin": int,  "step": int, "rango": int, "rpm": int, "rpm_azionam": int})
    df = df.reset_index(drop=True)
    #rimuove le zone di gestione doppia e compatta la poslin (funzione definita in crea_df.py)
    df = rimuovi_zone_gestione_doppia_e_compatta(df)
    return df
