package ILQ;

public class SpatialMetric {

    public static final  double EARTH_RADIUS = 6378137;
    public static final double MAX_LAT = 90.0;
    public static final double MIN_LAT = -90.0;
    public static final double MAX_LON = 180.0;
    public static final double MIN_LON = -180.0;
    public static final double MAX_SPATIAL_DIST = compute_dist(MAX_LAT , MAX_LON , MIN_LAT , MIN_LON);

    public static double rad(double d){
        return d * Math.PI / 180.0;
    }

    public static double compute_dist(double lat1 , double lon1, double lat2 , double lon2) {
        /*double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double a = radLat1 - radLat2;
        double b = rad(lon1) - rad(lon2);
        double s = 2 *Math.asin(Math.sqrt(Math.pow(Math.sin(a/2),2)+Math.cos(radLat1)*Math.cos(radLat2)*Math.pow(Math.sin(b/2),2)));
        s = s * EARTH_RADIUS;
        return s;*/
        double ld = 69.1 * (lat1 - lat2);
        double ll = 53 * (lon1 - lon2);
        return Math.sqrt(ld * ld + ll * ll);
    }

    public static double compute_dist_appro(double lat1 , double lon1 , double lat2 , double lon2)
    {
        double ld = 69.1 * (lat1 - lat2);
        double ll = 53 * (lon1 - lon2);
        return Math.sqrt(ld * ld + ll * ll);
    }
}
