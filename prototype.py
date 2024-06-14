from gensql.utils.connections import SQLiteColumnGetter
from gensql.utils.shmem import ShMemList
from gensql.worker import Worker
from gensql.generators.word_new import Word


def cleanup():
    for x in [shml_fname, shml_lname, shml_word]:
        x.shmem.shm.close()
        x.shmem.shm.unlink()

NUM_ROWS = 100_000

if __name__ == "__main__":
    sql = SQLiteColumnGetter
    max_fname, f_names = sql("first_name", "person_name").get_col()
    max_lname, l_names = sql("last_name", "person_name").get_col()
    max_word, words = sql("word", "word").get_col()

    shml_fname = ShMemList(
        shmem_name="first_name", create=True, sequence=f_names
    )
    shml_lname = ShMemList(
        shmem_name="last_name", create=True, sequence=l_names
    )
    shml_word = ShMemList(
        shmem_name="word", create=True, sequence=words
    )

    w_fname = Worker(NUM_ROWS, shml_fname.seeds, Word, "first_name")
    w_lname = Worker(NUM_ROWS, shml_lname.seeds, Word, "last_name")
    w_word = Worker(NUM_ROWS, shml_word.seeds, Word, "word")

    fnames = w_fname.generate_words()
    lnames = w_lname.generate_words()
    words = w_word.generate_words()

    breakpoint()

    cleanup()
