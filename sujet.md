# Subject

Le but du projet est de développer un programme batch java qui devra implémenter les fonctionnalités suivantes:

- scruter un dossier particulier, à la recherche des fichiers users_<YYYYMMDDHHmmSS>.csv

- parser ces fichiers qui sont au format :
<Numero_Securite_Sociale>, <Nom>, <Prenom>, <Date_Naissance>, <Numero_Telephone>, <E_Mail>, <ID_Remboursement>, <Code_Soin>, <Montant_Remboursement>

- peuple une base de données relationnelle avec ces entrées : 
  - l'ID remboursement est un identifiant qui permet de déterminer s'il s'agit d'un insert ou d'un update
  - une colonne <Timestamp_fichier> est extraite du nom du fichier

- une fois traités les fichiers sont déplacés dans un autre dossier

- les lignes en erreur (format incorrect) sont déplacés dans un dossier de rejet, dans un fichier avec le timestamp du fichier d'origine  

- optionnel : les fichiers users_<YYYYMMDDHHmmSS>.json peuvent également être supportés.

# Modalités

Le projet doit être fait par groupe de 2.

Les équipes sont les suivantes : 

- Equipe 1 : ABDOULKARIM	Issa-Mohamed / PEREIRA	Jonathan
- Equipe 2 : DUPONT	Charles / REOT	Maelo
- Equipe 3 : FALL	Ngnague / HANNOTTE	Guillaume
- Equipe 4 : HASSAYOUN	Syrine / LINARD	Victor	
- Equipe 5 : LAICHE	Elyes / RICHAUDEAU	Maxime
- Equipe 6 : LAMOUILLE	Lilian / MAISONNETTE	Mathieu	
- Equipe 7 : MUGUET	Julien / REMOND	Matthieu	
- Equipe 8 : RICHARD	Quentin / YARO	Eugène Pascal

## Stack technique

Le projet doit être fait en Java, avec Maven pour le build.
Des tests unitaires peuvent (doivent) être implémentés avec des Junit.
Des dépendances peuvent (doivent) être inclues, ne serait-ce que le driver JDBC pour la base (PostgreSQL recommandée).


## Livrables

Les livrables attendus via Mootse ou WeTransfer sont : 

- un zip avec le code source du programme : note sur 14

- un document technique rédigé expliquant les choix d'implémentation (un readme sera OK) : note sur 6

Date attendue : 11/04/2022 00:00.
