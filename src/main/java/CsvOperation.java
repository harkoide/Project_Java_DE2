import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CsvOperation {

    public static  ArrayList<String> scan_repositories(File path_in, File path_out, File path_error) throws  FileNotFoundException{
        ArrayList<String> file_error = new ArrayList<>();
        ArrayList<String> file_move_error = new ArrayList<>();
        ArrayList<String> file_move_success = new ArrayList<>();
        ArrayList<String> good_file = new ArrayList<>();
        // permet de lister les différent fichiers/repertoire de ce répertoire
        File[] liste = path_in.listFiles();
        Pattern p = Pattern.compile("^users_\\d{14}.csv");
        //boucle qui parcourt tous les résultats et avec un if pour le format d'écriture en fonction de si l'on a un fichier ou un repertoire
        for(File item : liste){
            if(item.isFile()) {
                Matcher m = p.matcher(item.getName());
                if(m.find()) {
                    System.out.println("Fichier correct dans le répertoire : "+ item.getName());
                    good_file.add(item.getName());
                }
                else{
                    file_error.add(item.getName());
                }
            }
            else if(item.isDirectory()) {
                System.out.format("Nom du répertoire: %s%n", item.getName());
            }
        }

        for(String file :file_error) {
            try {
                Files.move(Paths.get(path_in+"/"+file),Paths.get(path_error+"/"+file));
                file_move_success.add(file);
            } catch (IOException e) {
                file_move_error.add(file);
            }
        }
        if(!file_move_success.isEmpty()){
            System.out.println("Fichier incorrect déplacer dans le répertoire Error : "+ file_move_success);
        }
        if(!file_move_error.isEmpty()){
            System.err.println("Erreur de déplacement du/des fichier(s) csv : " + file_move_error);
        }

        return good_file;
    }


    public static ArrayList<String[]> read_csv(ArrayList<String> csv_list, String path) throws FileNotFoundException {
        ArrayList<String[]> line_error = new ArrayList<>();
        ArrayList<String[]> line_success = new ArrayList<>();
        String PathError = "";
        String[] path_error = path.split("/");
        boolean sortir = false;
        for (int i = 0; i < path_error.length-1; i++) {
            PathError += path_error[i]+"/";
        }
        for (String csv: csv_list) {
            File file = new File(path+"/"+csv);
            try(CSVReader csvreader = new CSVReader(new FileReader(file))) {
                String[] lineInArray;
                String[] lineInArraySucess;
                while ((lineInArray = csvreader.readNext()) != null) {
                    if(!(lineInArray[0].length()> 15)) {
                        sortir = false;
                        for (int i = 0; i < 9; i++) {
                            Pattern p;
                            Matcher m;
                            switch (i) {
                                case 0:
                                    p = Pattern.compile("^[0-1]\\d{14}");
                                    m = p.matcher(lineInArray[i]);
                                    if (!m.find()) {
                                        sortir = true;
                                    }
                                    break;
                                case 1:
                                case 2:
                                    p = Pattern.compile("\\D");
                                    m = p.matcher(lineInArray[i]);
                                    if (!m.find()) {
                                        sortir = true;
                                    }
                                    break;
                                case 3:
                                    p = Pattern.compile("\\d{2}\\/\\d{2}\\/\\d{4}");
                                    m = p.matcher(lineInArray[i]);
                                    if (!m.find()) {
                                        sortir = true;
                                    }
                                    break;
                                case 4:
                                    p = Pattern.compile("^0\\d{9}");
                                    m = p.matcher(lineInArray[i]);
                                    if (!m.find()) {
                                        sortir = true;
                                    }
                                    break;
                                case 5:
                                    p = Pattern.compile("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+.[A-Za-z]{2,4}$");
                                    m = p.matcher(lineInArray[i]);
                                    if (!m.find()) {
                                        sortir = true;
                                    }
                                    break;
                                case 6:
                                case 7:
                                case 8:
                                    p = Pattern.compile("\\d");
                                    m = p.matcher(lineInArray[i]);
                                    if (!m.find()) {
                                        sortir = true;
                                    }
                                    break;
                            }
                        }
                        if(!sortir){
                            String[] lineInArrayTimeStamp = add_date_csv_file(csv, lineInArray);
                            line_success.add(lineInArrayTimeStamp);
                        }
                        else{
                            line_error.add(lineInArray);
                        }

                    }


                }

                if(!line_error.isEmpty()){
                    try (CSVWriter writer = new CSVWriter(new FileWriter(PathError+"Error/"+csv))) {
                        writer.writeAll(line_error);
                    } catch (IOException e) {
                        e.printStackTrace();
                        System.err.println("Erreur d'écriture du fichier csv d'erreur.");
                    }
                }

            } catch (IOException | CsvValidationException e) {
                e.printStackTrace();
            }
            try{
                Files.move(Paths.get(path + "/" +csv),Paths.get(PathError+"out/"+csv));
            }catch (IOException e) {
                e.printStackTrace();
                System.err.println("Erreur lors du déplacement du fichier " + csv + "Dans le répertoire OUT");}


        }

        return line_success;
    }

    public static String[] add_date_csv_file(String csv_name, String[] lineInArray) throws FileNotFoundException {
        String[] lineInArrayTimeStamp = new String[lineInArray.length+1];
        String csv_date = csv_name.substring(6, 20);
        for (int i = 0; i < 9; i++) {
            lineInArrayTimeStamp[i]=lineInArray[i];
        }
        lineInArrayTimeStamp[9] = csv_date;
        return lineInArrayTimeStamp;
    }

    public static Timestamp csvdateToTimestamp(String csv_date) throws ParseException {
        DateFormat dateTimeFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        java.util.Date date = dateTimeFormat.parse(csv_date);
        long time = date.getTime();
        Timestamp timestamp = new Timestamp(time);
        return timestamp;
    }

}
