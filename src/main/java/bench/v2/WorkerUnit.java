package bench.v2;


@FunctionalInterface
public interface WorkerUnit {
	void iterate(WorkerState state);
}
