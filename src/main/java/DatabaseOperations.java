import java.sql.*;
import java.util.ArrayList;

public class DatabaseOperations {
    private String server;
    private String database;
    private String user;
    private String password;
    private String port;
    private Connection con;
    private Statement stmt;
    private ResultSet rs;

    public DatabaseOperations(String server, String database, String user, String password, String port){
        this.server = server;
        this.database = database;
        this.user = user;
        this.password = password;
        this.port = port;
    }

    public void DatabaseConnection() {
        try{
            Class.forName ("org.postgresql.Driver");
            System.out.println("Chargement du driver");
        }
        catch (ClassNotFoundException e) {System.err.println("Erreur de chargement du driver.");}

        try {
            this.con = DriverManager.getConnection("jdbc:postgresql://"+this.server+":"+this.port+"/"+this.database, this.user, this.password);
            System.out.println("Connexion réussie : " + this.con);
        } catch(SQLException e) {System.err.println("Erreur lors de la connexion à la base de données.");}
    }


    public void DatabaseStatement() throws SQLException {
        try {
            this.stmt=con.createStatement();
            System.out.println("Statement créé");
        }catch (SQLException e){System.err.println("Erreur, création du statement impossible");}

    }

    void DatabaseCloseConnection() throws SQLException {
        try {
            //System.out.println(this.con);
            this.con.close();
            this.con = null;
            this.stmt = null;
            System.out.println("Fermeture de la connection");
        }catch (SQLException e){System.err.println("Erreur lors de la fermeture de la connection");}
    }

    public boolean idRemboursementInDatabase(String id) {
        try {
            PreparedStatement pstmt = this.con.prepareStatement("SELECT COUNT(\"ID_remboursement\") AS count_ID_remboursement FROM \"Java_project\".\"JavaProject\" WHERE \"ID_remboursement\" = ?;");
            pstmt.setString(1, id);
            ResultSet rs = pstmt.executeQuery();
            if(rs.next()) {
                if(rs.getInt("count_ID_remboursement") > 0){
                    return true;
                }else{
                    return false;
                }
            }
            return false;
        } catch(SQLException e) {System.err.println("Erreur lors de l'éxécution de la requête pour chercher un ID de remboursement.");}
        return false;
    }


    void InsertOrUpdateInDataBase(ArrayList<String[]> line_csv_to_process) throws SQLException {
        try {

            for (String[] line_to_insert:line_csv_to_process) {
                if (!idRemboursementInDatabase(line_to_insert[6]))
                {
                    PreparedStatement pstmt = this.con.prepareStatement("INSERT INTO \"Java_project\".\"JavaProject\" (numero_securite_sociale, prenom, nom, date_naissance, numero_telephone, e_mail, \"ID_remboursement\", code_soin, montant_remboursement, \"Timestamp_fichier\") VALUES (?,?,?,?,?,?,?,?,?,?);");
                    pstmt.setString(1, line_to_insert[0]);
                    pstmt.setString(2, line_to_insert[1]);
                    pstmt.setString(3, line_to_insert[2]);
                    pstmt.setString(4, line_to_insert[3]);
                    pstmt.setString(5, line_to_insert[4]);
                    pstmt.setString(6, line_to_insert[5]);
                    pstmt.setString(7, line_to_insert[6]);
                    pstmt.setString(8, line_to_insert[7]);
                    pstmt.setString(9, line_to_insert[8]);
                    pstmt.setString(10, line_to_insert[9]);
                    pstmt.executeUpdate();
                }else{
                    PreparedStatement pstmt = this.con.prepareStatement("UPDATE \"Java_project\".\"JavaProject\" SET numero_securite_sociale=?, prenom=?, nom=?, date_naissance=?, numero_telephone=?, e_mail=?, \"ID_remboursement\"=?, code_soin=?, montant_remboursement=?, \"Timestamp_fichier\"=? WHERE \"ID_remboursement\" =?;");
                    pstmt.setString(1, line_to_insert[0]);
                    pstmt.setString(2, line_to_insert[1]);
                    pstmt.setString(3, line_to_insert[2]);
                    pstmt.setString(4, line_to_insert[3]);
                    pstmt.setString(5, line_to_insert[4]);
                    pstmt.setString(6, line_to_insert[5]);
                    pstmt.setString(7, line_to_insert[6]);
                    pstmt.setString(8, line_to_insert[7]);
                    pstmt.setString(9, line_to_insert[8]);
                    pstmt.setString(10, line_to_insert[9]);
                    pstmt.setString(11, line_to_insert[6]);
                    pstmt.executeUpdate();
                }

            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }

    public Connection getCon() {
        return this.con;
    }

    public Statement getStmt() {
        return this.stmt;
    }

}
