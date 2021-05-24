package mongoBeadando;

import java.util.concurrent.TimeUnit;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class mongoBeadando {
	public mongoBeadando() {
		super();
	}
	
	public static void main(String[] args) {
		mongoBeadando progi = new mongoBeadando();
		//progi.dropCollection("dolgozok");
		
		progi.feltoltesDolgozok(1, "Laci", "mernok", 6000.0, 3000.0);
		progi.feltoltesDolgozok(2, "Sandor", "mernok", 6700.0, 3400.0);
		progi.feltoltesDolgozok(3, "Beata", "Asszisztens", 4000.0, 1500.0);
		progi.feltoltesDolgozok(4, "Bela", "Technikus", 4300.0, 3200.0);
		
		progi.lekerdezesDolgozo(1);
		
		progi.fizetesKevesebb(4500.00);
	}
	
	
	@SuppressWarnings("deprecation")
	public void dropCollection(String collection) {
		try {
			MongoClient mongo = new MongoClient ("localhost", 27777);
			DB db = mongo.getDB("ABM2");
			DBCollection table = db.getCollection(collection);
			table.drop();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	@SuppressWarnings("deprecation")
	public void feltoltesDolgozok(int id, String nev, String beosztas, double fizetes, double jutalom) {
		try {
			MongoClient mongo = new MongoClient ("localhost", 27777);
			DB db = mongo.getDB("ABM2");
			DBCollection table = db.getCollection("dolgozok");
			
			BasicDBObject doksi = new BasicDBObject();
			doksi.put("_id", id);
			doksi.put("nev", nev);
			doksi.put("beosztas", beosztas);
			doksi.put("fizetes", fizetes);
			doksi.put("jutalom", jutalom);
			table.insert(doksi);
			//TimeUnit.SECONDS.sleep(1);
		} catch (Exception e) {
			//System.out.println(e.getMessage());
			if (e.getMessage().contains("11000")) {
				System.out.println("Mar van ilyen ID: " + Integer.toString(id));
			}
		}
	}
	
	
	@SuppressWarnings("deprecation")
	public void lekerdezesDolgozo(int id) {
		try {
			MongoClient mongo = new MongoClient ("localhost", 27777);
			DB db = mongo.getDB("ABM2");
			DBCollection table = db.getCollection("dolgozok");
			
			BasicDBObject lekerdezes = new BasicDBObject();
			lekerdezes.put("_id", id);
			DBCursor cursor = table.find(lekerdezes);
			while (cursor.hasNext()) {
				System.out.println(cursor.next());
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	@SuppressWarnings("deprecation")
	public void fizetesKevesebb(double fizu) {
		try {
			MongoClient mongo = new MongoClient ("localhost", 27777);
			DB db = mongo.getDB("ABM2");
			DBCollection table = db.getCollection("dolgozok");
			
			BasicDBObject allekerdezes = new BasicDBObject();
			allekerdezes.put("$lt", fizu);
			BasicDBObject lekerdezes = new BasicDBObject();
			lekerdezes.put("fizetes", allekerdezes);
			DBCursor cursor = table.find(lekerdezes);
			while (cursor.hasNext()) {
				System.out.println(cursor.next());
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

}
