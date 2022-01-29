import time
from threading import Thread
from typing import Any, Callable, Dict, List


class Parallel:
    """A class to run a function with multiple inputs in parallel."""

    def __init__(
        self,
        func: Callable,
        kwargs_list: List[Dict[str, Any]],
    ) -> None:
        self.func = func
        self.kwargs_list = kwargs_list
    
    def run(self, interval_secs: float = 0.0) -> None:
        """runs the parallel function

        Parameters
        ----------
        interval_secs : float, optional
            intervals between running threads, by default 0.0
            (set 0.05 seconds for http requests)
        """
        threads = [
            Thread(target=self.func, kwargs=kwargs)
            for kwargs in self.kwargs_list
        ]
        for thread in threads:
            thread.start()
            time.sleep(interval_secs)
        for thread in threads:
            thread.join()


if __name__ == "__main__":
    def print_hello(name: str) -> None:
        print(f"hello {name}")
        time.sleep(1)

    kwargs_list = [
        {"name": "John"},
        {"name": "Mike"}, 
        {"name": "Steve"},
        {"name": "Henry"},
        {"name": "Kate"},
        {"name": "Mary"},
        {"name": "Jack"},
        {"name": "Peter"},
        {"name": "John"},
        {"name": "Tom"},
    ]
    Parallel(print_hello, kwargs_list).run()
        
