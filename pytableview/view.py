import asyncio
import curses
import math

from abc import abstractmethod
from aioitertools import iter, next
from typing import Any, Coroutine, Optional


class DataSource():
    def __init__(self):
        self._max_cols = 5

    @abstractmethod
    async def raw(self, i) -> dict:
        pass

    async def row(self, i) -> list:
        _raw = await self.raw(i)
        if _raw:
            return list(_raw.values())

    async def num_cols(self) -> int:
        _row = await self.row(0)
        return min(self._max_cols, len(_row)) if _row else 0

    async def col_name(self, i) -> str:
        _raw = await self.raw(0)
        if _raw:
            return list(_raw.keys())[i]

    async def col_width(self, i) -> Optional[int]:
        return None


class FilterDataSource(DataSource):
    def __init__(self, upstream: DataSource, query: Coroutine[dict, Any, bool]):
        super().__init__()
        self.upstream = upstream
        self.query = query
        self.cache = []
        self.pointer = -1
        self.upstream_pointer = 0

    async def raw(self, i):
        while i > self.pointer:
            while True:
                item = await self.upstream.raw(self.upstream_pointer)
                if not item:
                    return None

                self.upstream_pointer += 1
                if await self.query(item):
                    self.cache.append(item)
                    self.pointer += 1
                    break

        return self.cache[i]


class IteratorDataSource(DataSource):
    def __init__(self, iterator):
        super().__init__()
        self.iterator = iter(iterator)
        self.cache = []
        self.pointer = -1

    async def raw(self, i):
        while i > self.pointer:
            try:
                item = await next(self.iterator)
                self.cache.append(item)
                self.pointer += 1  # only increment if no exception was thrown
            except StopAsyncIteration:
                return None

        return self.cache[i]


def match_text(text: str) -> Coroutine[dict, Any, bool]:
    search_text = str(text).lower()
    async def matcher(d: dict) -> bool:
        async for value in iter(d.values()):
            if search_text in str(value).lower():
                return True
        return False

    return matcher


class TableView():
    def __init__(self, loop, stdscr, data_source):
        self.loop = loop
        self.stdscr = stdscr
        self.data_source = data_source
        self.current_data_source = data_source
        self.selected_row = 0
        self.search_text = ""

    async def search(self):
        text = self.search_text
        if text and len(text) > 0:
            self.current_data_source = FilterDataSource(self.data_source, match_text(text))
        else:
            if self.current_data_source != self.data_source:
                self.current_data_source = self.data_source
        await self.draw()

    async def show(self):
        await self.draw()
        selected = await self.handle_keyboard()
        if selected:
            return await self.current_data_source.raw(self.selected_row)
        else:
            return None

    async def col_width(self, i):
        num_cols = await self.data_source.num_cols()
        col_widths = [await self.data_source.col_width(i) async for i in range(num_cols)]


    async def refresh_after(self, timeout=1):
        await asyncio.sleep(timeout)
        self.stdscr.refresh()

    async def draw(self):
        height, width = self.stdscr.getmaxyx()

        await self.clear(height, width)
        await asyncio.wait([
            self.draw_table(height, width),
            self.draw_prompt(height, width)
        ])

        self.stdscr.move(height - 1, len(self.search_text) + len("Search: "))

    async def clear(self, height, width):
        if curses.is_term_resized(height, width):
            curses.resizeterm(height, width)  # FIXME: Crash if less characters in new window size

        self.stdscr.clear()

    async def draw_prompt(self, height, width):
        col_attr = curses.color_pair(0)
        value = str(f"Search: {self.search_text}")
        self.stdscr.insnstr(
            height - 1,
            0,
            value.ljust(width),
            width,
            col_attr
        )

    async def draw_table(self, height, width):
        rows = height - 2
        min_row_i = max(0, self.selected_row - rows + 1)
        max_row_i = min_row_i + rows
        num_cols = await self.data_source.num_cols()
        cw = math.floor(width / max(1, num_cols))  # prevent division by zero

        for i in range(num_cols):
            self.stdscr.insnstr(
                0, i * cw,
                str(await self.data_source.col_name(i)).ljust(cw),
                cw,
                curses.A_BOLD + curses.color_pair(32 + i % 2)
            )

        for i in range(min_row_i, max_row_i):
            task = asyncio.Task(self.refresh_after(.1), loop=self.loop)
            row = await self.current_data_source.row(i)
            task.cancel()

            row_attr = curses.A_BOLD if self.selected_row == i else 0

            for j in range(num_cols):
                col_attr = curses.color_pair(34 + j % 2)
                value = str(row[j]) if row else ''
                self.stdscr.insnstr(
                    i + 1 - min_row_i,
                    j * cw,
                    value.ljust(cw),
                    cw,
                    row_attr + col_attr
                )

    async def movedown(self):
        if await self.current_data_source.row(self.selected_row + 1):
            self.selected_row += 1
            await self.draw()

    async def moveup(self):
        if self.selected_row >= 1:
            self.selected_row -= 1
            await self.draw()

    async def handle_keyboard(self):
        while True:
            key = await self.loop.run_in_executor(None, self.stdscr.getch)
            if key == curses.KEY_RESIZE:
                await self.draw()
            elif key == 258:
                await self.movedown()
            elif key == 259:
                await self.moveup()
            elif key in [27]:  # esc
                return False
            elif key in [10]:  # enter
                return True
            elif key in [263]:  # backspace
                self.search_text = self.search_text[:-1]
                await self.search()
            else:
                self.search_text += chr(key)
                await self.search()
