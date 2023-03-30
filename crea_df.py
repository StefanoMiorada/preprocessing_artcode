import pandas as pd
import re
import numpy as np
import hashlib

# hash su tutto l'artcode
def get_full_artcode_hash(filename):
  with open(filename, "rb") as f:
    file_content = f.read()
      # calcola l'hash del contenuto del file
  hash_hex = hashlib.sha256(file_content).hexdigest()
  return hash_hex

#hash solo su una parte dell'artcode.
#memorizzo in output_text la parte dell'artcode compresa tra "tipo=EXEC_BACKG" e "tipo=EXEC_TIMER"
#successivamente faccio l'hash solo di questa parte
def get_splitted_artcode_hash(filename):
  output_lines = []
  found_start = False
  with open(filename, 'r') as input_file:
    for line in input_file:
      if "tipo=EXEC_BACKG" in line:
        found_start = True
      if found_start:
        if "tipo=EXEC_TIMER" in line:
          break
        else:
          output_lines.append(line)
    output_text = "".join(output_lines)
    hash = hashlib.sha256(output_text.encode()).hexdigest()
  return hash

#<--key: nome del visual da ricercare all'interno dell'artcode es.(step_actual,rango_actual)
#<--nome_colonna2: il nome che verrà assegnato alla seconda colonna del sub_df es.(step,rango)
#<--filename:il filename assoluto della posizione dell'artocde in questione
#-->df: ritorna un df composto da poslin e la colonna ricercata nei visual
#===funzione che ritorna un df composto da posLin e una colonna che varia in modo dinamico in base al blocco di codice letto (es. rango o step)
#===questa funzione è utilizzata per leggere dei sub_df che andranno poi a comporre il df completo
def crea_sub_df(key,nome_colonna2,filename):
    riga_artcode=[] #lista che contiene in ogni elemento una riga dell'artcode
    flag=False #per indicare quando sono all'interno del blocco ricercato
    # creo il pattern: inizia con [VISUAL] e termina con la key passata (es. step_actual) che non deve essere parte di una parola più grande
    pattern = re.compile(r"^\[VISUAL\].*\b" + re.escape(key) + r"\b", re.IGNORECASE)
    with open(filename, 'r') as f:
      for line in f:
        if re.search(pattern,line.strip()):
          flag=True #quando individuo il pattern metto a true il flag in modo da poter leggere le righe successive
        if line == "\n": #la lettura continua finchè non trovo una riga bianca
          flag=False
        if flag:
          if "VISUAL_STRING" in line: #effettuo un controllo aggiuntivo sull'inizio della riga
            if "-100000" not in line: #non volgio leggere le righe con posLin=-100000
              riga_artcode.append(line)
    #creo il dataframe. Uso una lista di appoggio per creare poi il dataframe solo alla fine
    df_rows = []
    for element in riga_artcode: #l'elemento è della forma VISUAL_STRING / posLin=0 / 0
      e=element.split("/")
      posLin = int(e[1].split("=")[1]) #prendo solo il valore di posLin e non la scritta
      if key.lower()=="nome_zona": #se sto trattando zone_name la seconda colonna è string altrimenti int
        colonna2 = str(e[2].strip()) #colonna2 è la colonna a cui verrà dato il nome passato come parametro
      else:
        colonna2 = int(e[2].strip())
      df_rows.append([posLin,colonna2])
    df=pd.DataFrame(df_rows,columns=["posLin",nome_colonna2])
    return df

#<-- df: df a cui aggiungere il rango relativo
#--> df: df con la colonna rango relativo aggiunta
#===funzione che dato un df aggiunge la colonna relative_course che indica il rango relativo all'interno dell'economia.
#===quando incontro una prima riga con econ=True assegno relative_course=1, le successive righe appartenenti all'economia avranno
#===un valore di relative_course incrementale. il contatore si resetta appena incontro una riga con econ=False
#===N.B. relative_course viene anche assegnato alle economie "sali-scendi" se non voglio questo devo spostare la chiamata
#===alla funzione prima di modificare econ in base a forstep_econ
def aggiungi_relative_course(df):
  df["relative_course"]=int(0)
  contatore=1
  for row in df.itertuples():
    if row.econ == True:
      df.loc[row.Index,"relative_course"]=contatore
      #row.relative_course=contatore
      contatore+=1
    else:
      contatore = 1
  return df

#<--df: dataframe con i merge e i fillna già effettuati
#-->lista_codici: una lista (di cui l'ordine è importante) che andrà a formare la colonna zone_code
#===funzione che ritorna una lista di codici (che sarà la colonna zone_code)
def mapping_zone_codici(df):
  codice=0
  map_codici=[]#associazione tra zone_name e zone_code da me assegnato
  nomi_zone=[]#lista d'appoggio usata per salvare la colonna delle zone
  nomi_zone_unique=[]#lista contenente il nome delel zone unique
  #popolo la lista nomi_zone che contiene i nomi di ogni zona. 2 casi poichè potrei non aver effettuato il preprocessing (quindi esiste la colonna zona del tipo 0-QUALCOSA) oppure potrei già averla separata nelle colonne zone_code e nome_zona
  if "zona" in df.columns:
    for e in df["zona"].str.split("-"):#prendo la colonna zona ed effettuo lo split per salvare nella lista nomi_zone
        nomi_zone.append(e[1])
  else:#la colonna zona è stata cancellata poichè è già stata seprarata in zone_code e zone_name
    nomi_zone=df.zone_name.values.tolist()
  #popolo la lista nomi_zone_unique. non uso set poihcè mi interessa mantenere l'ordine di apparizione
  for item in nomi_zone:
    if item not in nomi_zone_unique:
      nomi_zone_unique.append(item)
  #assegno manualmente ad ogni zone_name un codice univoco (ho dovuto fare cosi perchè in certi programma non c'era coerenza es. 0-FINE CALZA e 11-FINE CALZA)
  for i in nomi_zone_unique:
    x={"zone_code":codice,"zone_name":i}
    map_codici.append(x)
    codice+=1
  #creo un dict con chiave il nome della zona e valore il codice assegnato
  dict_codici_key_codice = {}
  for elemento in map_codici:
    dict_codici_key_codice[elemento['zone_name']] = elemento['zone_code']
  #aggiungo al df la colonna zone_name
  if "zone_name" not in df.columns:
    df["zone_name"]=pd.DataFrame(nomi_zone,columns=["zone_name"])
  #creo una lista di codici, per ogni zone_name ottenuto da iterrows cerco all'interno del dict_codici il relativo codice che poi appendo alla lista
  lista_codici = [dict_codici_key_codice[row.zone_name] for row in df.itertuples()]
  #fine
  return lista_codici

#<--df: df in cui individuare le zone di gestione doppia
#-->lista_zone_trovate: una lista di tuple (poslin di inizio zona , poslin di fine zona)
#===individua le zone di gestione doppia andando a cercare quei punti dove lo step diminuisce e forstep_econ != 0
#===la zona di gestione doppia finisce quando lo step ritorna uguale allo step precedente all'inizio della zona di gestione doppia
def trova_zone_gestione_doppia(df):
  lista_zone_trovate=[]
  for row in df.itertuples():
    index=getattr(row,"Index")
    if index != 0:
      actual_step=row.step
      previous_step=df.loc[index-1,"step"]
      if (actual_step<previous_step) and (row.forstep_econ == 0):
        previous_poslin = df.loc[index - 1, "posLin"]
        poslin_start = previous_poslin
        step_start = previous_step
        df_subset = df.loc[(df['posLin'] > poslin_start)]
        poslin_end = df_subset.loc[(df_subset['step'] == step_start), "posLin"].values[0]
        lista_zone_trovate.append((poslin_start, poslin_end))
  return lista_zone_trovate

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

#-->df: df a cui rimuovere le zone di gestione doppia
#<--df: df con zone di gestion doppia rimosse e poslin compattata
#===funzione che rimuove le zone di gestione doppia e compatta la posLin
def rimuovi_zone_gestione_doppia(df):
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

#-->df_to_change:il dataframe a cui modificare le posLin
#-->reference_df: il dataframe di riferimento, tipicamente step o rango
#<--df: il dataframe df reso compatibile a df_step
#===funzione che modifica la posLin di df in modo da renderla compatibile con df_step
def make_df_compatible_with_step(df_to_change,reference_df):
  df_to_change["posLin"] = df_to_change["posLin"].apply(
    lambda x: reference_df.loc[(reference_df['posLin'] - x).abs().argsort()[:1], "posLin"].values[0])
  return df_to_change

#<--filename: il nome del file da analizzare
#-->:df: dataframe finale (zone di gestione doppia rimosse e posizione lineare compatta e rimossa) con le seguenti colonne:
#    step,course,rpm,econ,forstep_econ (individua le economie "sali-scendi"),zone_name,zone_code,
#    relative_course(rango/passo relativo all'interno di un economia),timestamp,machine_id,program,hash
#===funzione principale che richiama tutte le altre funzioni
def crea_dataframe_rango_unico(filename):
  df_step = crea_sub_df("step_actual","step",filename)
  df_course = crea_sub_df("rango_actual","course",filename)
  df_rpm = crea_sub_df("rpm_program","rpm",filename)
  df_zona = crea_sub_df("nome_zona","zona",filename)
  df_econ = crea_sub_df("econom_end","econ",filename)
  df_art_forstep = crea_sub_df("ART_FORSTEP_ECONOM_ACTUAL", "forstep_econ",filename)
  #econ contiene originariamente il valore relativo all'interno dell'economia, io voglio True/False
  df_econ["econ"]=df_econ["econ"].apply(lambda x: True if x>1 else False)
  #Merge dei dataset con outer join in base a posLin
  df=pd.DataFrame()
  df=df_step.merge(df_course,how='outer',on="posLin",sort=True)
  #modifica df_rpm in modo da essere compatibile con step e rango e merge
  df_rpm_modificato=make_df_compatible_with_step(df_rpm,df)
  df=df.merge(df_rpm_modificato,how='outer',on="posLin",sort=True)
  #modifica df_zona in modo da essere compatibile con step e rango e merge
  df_zona_modificato=make_df_compatible_with_step(df_zona,df)
  df=df.merge(df_zona_modificato,how='outer',on="posLin",sort=True)
  #modifica df_econ in modo da essere compatibile con step e rango e merge
  df_econ_modificato=make_df_compatible_with_step(df_econ,df)
  df=df.merge(df_econ_modificato,how='outer',on="posLin",sort=True)
  # merge con df_forstep
  df = df.merge(df_art_forstep, how='outer', on="posLin", sort=True)
  #modifica della colonna econ in base alle economie individuate da forstep_econ.
  #se econ_forstep è diverso da zero significa che siamo all'interno di un economia sali-scendi
  df["econ"] = df.apply(lambda row: True if (row["econ"]==False and row["forstep_econ"]!=0) else row["econ"],axis=1)
  #aggiunta del rango relativo all'interno di un economia
  df=aggiungi_relative_course(df)
  #gestion NaN
  df=df.fillna(method='ffill') #ForwardFILL
  df=df.fillna(method='bfill') #BackwardFILL
  #mapping dei nomi zona con un codice numerico
  lista_codici=mapping_zone_codici(df)
  #aggiungo al df la colonna zone_code che è rappresentata dalla lista_codici appena creata
  df["zone_code"]=pd.DataFrame(lista_codici,columns=["zone_code"])
  #rimuovo la colonna zona (zone_code-zone_name) in quanto ho effettuato lo split in due colonne
  df=df.drop("zona",axis=1)
  #gestione delle righe con posLin duplicata: mantengo il primo elemento
  for valore_posLin in df['posLin'].unique():
      righe_duplicate = df[df['posLin'] == valore_posLin].duplicated(subset='posLin')
      if any(righe_duplicate):
        df = df.drop(df[df['posLin'] == valore_posLin][righe_duplicate].index[0])
  #effettua il casting altrimenti sarebbero float.
  df = df.astype({"rpm": int,"forstep_econ":int})
  df = df.reset_index(drop=True)
  #vengono rimosse le zone di gestione doppia e la poslin viene compattata
  df=rimuovi_zone_gestione_doppia(df)
  #rimuovi la colonna poslin che ora non serve più a nulla. non possibile rimuoverla prima perchè serve per rimozione
  df = df.drop("posLin", axis=1)
  #riordina le colonne
  columns_order = ['timestamp', 'machine_id', 'program','hash', 'step', 'course','relative_course', 'econ', 'forstep_econ','rpm', 'zone_code', 'zone_name']
  df = df.reindex(columns=columns_order)
  return df