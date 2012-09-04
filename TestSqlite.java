
public class TestSqlite {

    public static void main(String [] args) {
        try {
            System.out.println("os.name: " + System.getProperty("os.name"));
            Class< ? > nativedb = Class.forName("org.sqlite.NativeDB");
            if (((Boolean) nativedb.getDeclaredMethod("load", (Class< ? >[]) null).invoke((Object) null, (Object[]) null)).booleanValue())
                nativedb.newInstance();
            System.out.println("loaded!");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
