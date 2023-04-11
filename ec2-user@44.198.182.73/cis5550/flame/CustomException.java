package cis5550.flame;

public class CustomException extends Exception {
    private static final long serialVersionUID = 1L;

	public CustomException(String errorMessage) {
        super(errorMessage);
    }
}
