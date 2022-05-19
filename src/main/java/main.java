import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.regex.*;


public class main {
    public static void main(String[] args) throws FileNotFoundException, SQLException {
        //chemin du repertoire que l'on veut scanner
        File directory_In  = new File("src/main/resources/In");
        File directory_Out  = new File("src/main/resources/Out");
        File directory_Error = new File("src/main/resources/Error");
        ArrayList<String> good_file = fonctions.scan_repositories(directory_In, directory_Out, directory_Error);

        ArrayList<String[]> line_csv_to_process = fonctions.read_csv(good_file, directory_In.getPath());

        if(!line_csv_to_process.isEmpty()){
            DatabaseOperations db = new DatabaseOperations("localhost", "charlesdupont", "charlesdupont", "", "5432");
            db.DatabaseConnection();
            if(db.getCon() != null) {
                /* Création du statement */
                db.DatabaseStatement();
                db.InsertOrUpdateInDataBase(line_csv_to_process);
                /* Fermer la connection */
                db.DatabaseCloseConnection();
            }
        }
    }
}
