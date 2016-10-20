package Sort;

public class Glue {

	private String date;
	private String temp;
	public Glue() {
		super();
	}
	public Glue(String date, String temp) {
		super();
		this.date = date;
		this.temp = temp;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getTemp() {
		return temp;
	}
	public void setTemp(String temp) {
		this.temp = temp;
	}
	@Override
	public String toString() {
		return "Glue [date=" + date + ", temp=" + temp + "]";
	}
	
	
}
