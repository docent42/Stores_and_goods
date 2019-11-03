import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.util.Collections;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import static com.mongodb.client.model.Updates.*;

public class Main
{

    public static void main(String[] args) {

        try (MongoClient mongoClient = MongoClients.create())
        {
            TimeUnit.SECONDS.sleep(3);
            System.out.println("\n======================= MongoDB TESTER =============================\n" +
                    "\t\t\t( Для завершения программы напишите ВЫХОД_ОТСЮДА )\n");
            MongoDatabase mongoDB = mongoClient.getDatabase("test");
            MongoCollection<Document> stores = mongoDB.getCollection("stores");
            MongoCollection<Document> goods = mongoDB.getCollection("goods");
            String input, command, target, token1, token2;
            Scanner scanner = new Scanner(System.in);
            String errorExistingPosition = "\n\t[ Такая позиция уже существует ! ]";
            String errorWrongCommand = "\n\t[ Неверная команда ! ]";
            boolean exit = true;

            while (exit)
            {
                System.out.print("\nВведите команду ===> ");
                input = scanner.nextLine().trim();
                String[] tokens = input.split(" ");
                command = tokens[0].split("_")[0].toUpperCase();
                target = tokens[0].split("_")[1].toUpperCase();
                token1 = (tokens.length > 1) ? tokens[1] : "";
                token2 = (tokens.length > 2) ? tokens[2] : "";
                switch (command)
                {
                    case "ДОБАВИТЬ" : {
                        if (target.equals("ТОВАР")) {
                            if (!Optional.ofNullable(goods.find(new Document("name",token1)).first()).orElse(new Document()).containsValue(token1))
                                goods.insertOne(new Document("name",token1).append("price",Integer.parseInt(token2)));
                            else
                                System.out.println(errorExistingPosition);
                        }
                        if (target.equals("МАГАЗИН")) {
                            if (!Optional.ofNullable(stores.find(new Document("name",token1)).first()).orElse(new Document()).containsValue(token1))
                                stores.insertOne(new Document("name", token1).append("inStock",Collections.emptyList()));
                            else
                                System.out.println(errorExistingPosition);
                        }
                        break;
                    }
                    case "ВЫХОД" : { exit = false; break;}
                    default:
                        System.out.println(errorWrongCommand);
                }

            }

//            collection.insertOne(new Document().append("name","Perekrestok").append("inStock", Collections.emptyList()));
//            collection.findOneAndUpdate(new Document("name","Perekrestok"),addToSet("inStock", "vaffels"));
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }

    }

}
