# MapReduce Prototype
## Project Overview
This project implements a MapReduce solution to determine the passenger(s) having had the highest number of flights based on the provided flight and passenger data. The implementation uses Python's multiprocessing library to simulate the distributed processing nature of MapReduce on a single machine.

## Implementation Details
The solution follows the standard MapReduce paradigm:
- **Map Phase**: Processes chunks of passenger data in parallel, counting flights per passenger
- **Combine Phase**: Local aggregation of counts within each mapper
- **Shuffle Phase**: Grouping counts by passenger ID
- **Reduce Phase**: Final aggregation to determine total flights per passenger

## Project Structure
- `src/`
  - `mapreduce_taskA.py`: Main MapReduce implementation
- `data/`
  - `AComp_Passenger_data_no_error.csv`: Passenger flight data
  
- `report/`
  - `CSMBD-TaskA-Report.pdf`: Detailed report on the implementation

## How to Run
shell 
python src/mapreduce_taskA.py data/AComp_Passenger_data_no_error.csv

## Output
The program outputs the passenger(s) with the highest number of flights, including:
- The maximum number of flights taken by any passenger
- A list of all passengers who achieved this maximum count

## Requirements
- Python 3.6+
- NumPy

## Performance
The solution is optimized for parallel processing and can efficiently handle large datasets by:
- Dividing work across available CPU cores
- Using memory-efficient data structures
- Implementing local combining to reduce shuffle overhead

## Version Control
This project is version controlled using Git, with the development history showing the iterative implementation of the MapReduce solution.

