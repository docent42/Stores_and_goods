import com.google.gson.Gson;
import com.mongodb.client.*;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Accumulators.first;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;


public class Main
{

    public static void main(String[] args) {

        try (MongoClient mongoClient = MongoClients.create())
        {
//================================= INIT ===============================================================
            // -------------- Connection ---------------------------------------------------------

            TimeUnit.SECONDS.sleep(3);
            MongoDatabase mongoDB = mongoClient.getDatabase("test");
            MongoCollection<Document> stores = mongoDB.getCollection("stores");
            MongoCollection<Document> goods = mongoDB.getCollection("goods");

            //---------------------- Шапка  -----------------------------------------------

            System.out.println("В базе:\n\tмагазинов - " + stores.countDocuments() + "\n\tтоваров - " + goods.countDocuments());
            System.out.println("\n======================= MongoDB TESTER =============================\n" +
                    "\t\t\t( Для завершения программы напишите ВЫХОД_ОТСЮДА )\n");
            String input, command, target, token1, token2;
            Scanner scanner = new Scanner(System.in);
            String errorExistingPosition = "\n\t[ Такая позиция уже существует ! ]";
            String errorWrongCommand = "\n\t[ Неверная команда ! ]";
            String errorNonExistingPosition = "\n\t[ Несуществующий товар или магазин ! ]";
            boolean exit = true;

// =================================== ЦИКЛ - Консоль ========================================================
            while (exit)
            {
            // ------------------------- Парсинг ввода --------------------------------------

                System.out.print("\nВведите команду ===> ");
                input = scanner.nextLine().trim();
                String[] tokens = input.split("\\s+");
                command = tokens[0].split("_")[0].toUpperCase();
                target = tokens[0].split("_")[1].toUpperCase();
                token1 = (tokens.length > 1) ? tokens[1] : "";
                token2 = (tokens.length > 2) ? tokens[2] : "";

             // ---------------- Выбор действия по команде ------------------------------------
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
                    case "ВЫСТАВИТЬ" : {
                        System.out.println("good: " + token1);
                        System.out.println("store: " + token2);
                        if ((Optional.ofNullable(goods.find(new Document("name",token1)).first()).orElse(new Document()).containsValue(token1))
                        && (Optional.ofNullable(stores.find(new Document("name",token2)).first()).orElse(new Document()).containsValue(token2)))
                            stores.findOneAndUpdate(new Document("name",token2), Updates.addToSet("inStock", token1));
                        else
                            System.out.println(errorNonExistingPosition);
                        break;
                    }
                    case "СТАТИСТИКА" : {
                        getStatistic(stores);
                        break;
                    }
                    case "ВЫХОД" : { exit = false; break;}
                    default:
                        System.out.println(errorWrongCommand);
                }
            }
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }
    private static void getStatistic(MongoCollection<Document> collection) {

        //---------------------- Формирование запроса ----------------------------------------------------

        AggregateIterable<Document> output = collection.aggregate(Arrays.asList(
                unwind("$inStock"),
                lookup("goods","inStock","name", "item"),
                addFields(new Field<>("count",1)),
                unwind("$item"),
                addFields(new Field<>("price1","$item.price")),
                project(fields(include("name", "inStock","price1","count"))),
                group("$name", avg("averagePrice", "$price1"),sum("count","$count"),
                        min("minPrice","$price1"), max("maxPrice","$price1"),
                        push("goods","$price1"))
                ,unwind("$goods"),match(lt("goods", 100)),
                addFields(new Field<>("count1",1))
                ,group("$_id",sum("countLt100","$count1"),first("avgPrice","$averagePrice")
                        ,first("countTotal","$count"),first("minPrice","$minPrice")
                        ,first("maxPrice","$maxPrice"))
                ,addFields(new Field<>("storeId","$_id"))
        ));
        // ------------------------- Парсинг ответа БД и выод результатов -------------------------------------

        for (Document dbObject : output)
        {
         Store temp = new Gson().fromJson(dbObject.toJson(), Store.class);

                System.out.printf("%n********* < %s > *********%n" +
                                "%n— Общее количество товаров: %s" +
                                "%n— Средняя цена товара:      %s" +
                                "%n— Самый дорогой товар:      %s" +
                                "%n— Самый дешевый товар:      %s" +
                                "%n— Количество товаров,%n  дешевле 100 рублей:       %s%n%n",
                        temp.storeId,temp.countTotal,temp.avgPrice,temp.maxPrice,temp.minPrice,temp.countLt100);

        }
    }

}
