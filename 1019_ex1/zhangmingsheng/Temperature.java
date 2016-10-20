package join;

public class Temperature {
	private String date;
	private int temperature;

	public Temperature() {
		super();
	}

	public Temperature(String date, int temperature) {
		super();
		this.date = date;
		this.temperature = temperature;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public int getTemperature() {
		return temperature;
	}

	public void setTemperature(int temperature) {
		this.temperature = temperature;
	}

}
