public class FUNCSTRING {
    public static void main(String[] args) {
        StringBuilder check_text = new StringBuilder("bbbbbcccccccccccccc");
        check_text.delete(0,1);
        StringBuilder str_bld = new StringBuilder();
        str_bld.append("aaaaaaaaaaaaa");
        str_bld=check_text;
        System.out.println(str_bld);
        System.out.println(check_text);
    }
}
