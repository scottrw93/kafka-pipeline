package kafka.pipeline;

import java.io.IOException;

public abstract class StepHook {
	private Integer id;
	
	/**
	 * A hook is a step of user defined code that is executed  in the pipeline that
	 * is executed in the pipeline. The steps are executed in order of id and are 
	 * repeatedly executed until all messages have been consumed.
	 * @param id
	 */
	public StepHook(Integer id){
		this.id = id;
	}
	
	/**
	 * The id of this Hook
	 * @return - id of this Hook
	 */
	public Integer id(){
		return id;
	}
	
	/**
	 * The code that will be executed in the pipeline for this hook.
	 * @param pipe
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public abstract void execute(PipeLine pipe) throws ClassNotFoundException, IOException;
}
