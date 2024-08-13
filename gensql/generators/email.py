from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, List, Optional

from gensql.core.constants import (
    DEFAULT_GENERATE_CHUNK_SIZE,
    MAX_EMAIL_ADDR_LEN,
    MAX_EMAIL_DOMAIN_LEN,
    MAX_EMAIL_LOCAL_LEN,
)
from gensql.generators.word import WordGenerator

if TYPE_CHECKING:
    from gensql.utils.shuffleable_bytes import ShuffleableBytes


class EmailFmt:
    de_dupe_with_nums: bool = True  # TODO: implement
    max_len_domain: int = 0  # TODO: implement
    max_len_fname: int = 0
    max_len_lname: int = 0
    max_len_local: int = 0  # TODO: implement
    lname_first: bool = False
    local_separator: str = "."
    use_fname: bool = True
    use_lname: bool = True
    use_random: bool = False  # TODO: implement

    def __post_init__(self):
        _errors = []
        show_rfc = True
        if not self.use_fname | self.use_lname | self.use_random:
            show_rfc = False

            _errors.append("Must have at least one of fname, lname, random")
        if (
            self.max_len_fname + self.max_len_lname > self.max_len_local
            and self.max_len_local
        ):
            _errors.append(
                f"Combined length of max_len_fname ({self.max_len_fname}) and max_len_lname ({self.max_len_lname}) cannot exceed that of max_len_local ({self.max_len_local})"
            )
        if self.max_len_domain > MAX_EMAIL_DOMAIN_LEN:
            _errors.append(
                f"Maximum length of domain of email address must be <= {MAX_EMAIL_DOMAIN_LEN}: must have 1 reserved for separator, and a minimum length of 1 in local-part"
            )
        if self.max_len_local > MAX_EMAIL_LOCAL_LEN:
            _errors.append(
                f"Maximum length of local-part of email address must be <= {MAX_EMAIL_LOCAL_LEN} characters"
            )
        if self.max_len_local + self.max_len_domain > MAX_EMAIL_ADDR_LEN:
            _errors.append(
                f"Maximum length of local-part and domain of email address must be <= {MAX_EMAIL_ADDR_LEN} characters"
            )
        if _errors and show_rfc:
            _errors.append("See RFC3696 and Errata 1690")
        if _errors:
            raise ValueError("\n".join(_errors))


class EmailFmtDefSection:
    __slots__ = ["name", "length"]

    def __init__(self, name: str, length: int):
        self.name = name
        self.length = length


class EmailFmtDef:
    __slots__ = ["local_separator", "sections"]

    def __init__(
        self, local_separator: str, sections: List[Optional[EmailFmtDefSection]]
    ):
        self.local_separator = local_separator
        self.sections = sections


class DomainGenerator:
    def __init__(
        self,
        byte_list: Optional[ShuffleableBytes],
        domain: str = "",
        generator_name: str = "domain",
        is_lower: bool = False,
        chunk_size: int = DEFAULT_GENERATE_CHUNK_SIZE,
    ):
        self.byte_list = byte_list
        self.chunk_size = chunk_size
        self.static_domain = domain.encode("utf-8") if domain else None

        self.word_generator = (
            WordGenerator(byte_list, is_lower, chunk_size, generator_name)
            if byte_list
            else None
        )

    def set_shuffle_callback(self, callback) -> None:
        if self.word_generator:
            self.word_generator.set_shuffle_callback(callback)

    def generate(self, num_rows: int) -> Iterable[List[bytes]]:
        if self.static_domain:
            for i in range(0, num_rows, self.chunk_size):
                chunk_size = min(self.chunk_size, num_rows - i)
                yield [self.static_domain] * chunk_size
        elif (
            self.word_generator
            and self.byte_list
            and self.word_generator.shuffle_callback
        ):
            seed_func = (
                self.word_generator.shuffle_callback.get("domain")
                or self.word_generator.shuffle_callback["default"]
            )
            seed_func(self.byte_list.indices)
            yield from self.word_generator.generate(num_rows)
        else:
            raise ValueError("Either static domain or byte_list must be provided")

    def reset_indices(self):
        if self.word_generator:
            self.word_generator.reset_indices()


class EmailGenerator:
    def __init__(
        self,
        fname_generator: Any,
        lname_generator: Any,
        domain_generator: Any,
        email_fmt: EmailFmt,
        chunk_size: int = DEFAULT_GENERATE_CHUNK_SIZE,
    ):
        self.fname_generator = fname_generator
        self.lname_generator = lname_generator
        self.domain_generator = domain_generator
        self.chunk_size = chunk_size
        email_fmt_def = self._make_email_fmt_def(email_fmt)
        self.email_fmt = email_fmt
        self.format_str = self._make_email_fmt_str(email_fmt_def)

    def _make_email_fmt_str(self, email_fmt_def: EmailFmtDef) -> bytes:
        """Creates a printf-style format string for producing emails.
        Creates a printf-style format string for producing emails.
        This was chosen over the dict-style purely for speed: it's ~20% faster.

        Examples:
            %b.%b@%b.com:
                [bytestring][byestring]@[bytestring.com
            %.5b.%b@%b.com:
                [bytestring, max_len=5][bytestring]@[bytestring].com
        """

        format_str: List[str] = []
        for i, key in enumerate(email_fmt_def.sections):
            if key is not None:
                if key.name != "domain":
                    format_str.append("%")
                if key.length > 0:
                    format_str.append(f".{key.length}")
                if key.name != "domain":
                    format_str.append("b")
                if i == 0:
                    format_str.append(email_fmt_def.local_separator)
                # TODO: add other TLDs
                if key.name == "domain":
                    format_str.append("@%b.com")
        return "".join(format_str).encode("utf-8")

    def _make_email_fmt_def(self, email_fmt: EmailFmt) -> EmailFmtDef:
        indices = [1, 0, 2] if email_fmt.lname_first else [0, 1, 2]
        email_fmt_sections: List[Optional[EmailFmtDefSection]] = []
        email_fmt_sections.append(
            EmailFmtDefSection("fname", email_fmt.max_len_fname)
            if email_fmt.use_fname
            else None
        )
        email_fmt_sections.append(
            EmailFmtDefSection("lname", email_fmt.max_len_lname)
            if email_fmt.use_lname
            else None
        )
        email_fmt_sections.append(
            EmailFmtDefSection("domain", email_fmt.max_len_domain)
        )
        email_fmt_sections = [
            email_fmt_sections[i] for i in indices if email_fmt_sections[i]
        ]

        return EmailFmtDef(email_fmt.local_separator, email_fmt_sections)

    def set_shuffle_callback(self, callback) -> None:
        self.fname_generator.set_shuffle_callback(callback)
        self.lname_generator.set_shuffle_callback(callback)
        self.domain_generator.set_shuffle_callback(callback)

    def reset_indices(self) -> None:
        self.fname_generator.reset_indices()
        self.lname_generator.reset_indices()
        self.domain_generator.reset_indices()

    def generate(self, num_rows: int) -> Iterable[List[bytes]]:
        fname_gen = self.fname_generator.generate(num_rows)
        lname_gen = self.lname_generator.generate(num_rows)
        domain_gen = self.domain_generator.generate(num_rows)

        for i in range(0, num_rows, self.chunk_size):
            chunk_size = min(self.chunk_size, num_rows - i)
            fnames = next(fname_gen)
            lnames = next(lname_gen)
            domains = next(domain_gen)

            if self.email_fmt.use_fname and self.email_fmt.use_lname:
                emails = [
                    (self.format_str % (fname[0].lower(), lname[0].lower(), domain),)
                    for fname, lname, domain in zip(fnames, lnames, domains)
                ]
            elif self.email_fmt.use_lname:
                emails = [
                    (self.format_str % (lname[0].lower(), domain),)
                    for lname, domain in zip(lnames, domains)
                ]
            elif self.email_fmt.use_fname:
                emails = [
                    (self.format_str % (fname[0].lower(), domain),)
                    for fname, domain in zip(fnames, domains)
                ]
            yield emails
