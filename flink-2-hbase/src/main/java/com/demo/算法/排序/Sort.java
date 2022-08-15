package com.demo.算法.排序;

import java.util.*;

public class Sort {


    /**
     * List<Song>  rank(List<Song>  songs, List<Integer>  ids)
     * s2 s1  s3 s4  s6 s5   s8 s7       1, 2, 3, 5, 9
     * s1 s2 s3 s5 s4 s6 s8 s7
     * @param args
     */
    public static void main(String[] args) {
        List<Integer> songs = new ArrayList<>();
        List<Song> songs_2 = new ArrayList<>();

        List<Integer>  ids = new ArrayList<>();

        songs.add(2);
        songs.add(1);
        songs.add(3);
        songs.add(4);
        songs.add(6);
        songs.add(5);
        songs.add(8);
        songs.add(7);



        songs_2.add(new Song(2,"s2"));
        songs_2.add(new Song(1,"s1"));
        songs_2.add(new Song(3,"s3"));
        songs_2.add(new Song(4,"s4"));
        songs_2.add(new Song(6,"s6"));
        songs_2.add(new Song(5,"s5"));
        songs_2.add(new Song(8,"s8"));
        songs_2.add(new Song(7,"s7"));


        ids.add(1);
        ids.add(2);
        ids.add(3);
        ids.add(5);
        ids.add(9);

        rank(songs, ids);
        System.out.println("----------");
        rank_2(songs_2, ids);

    }


    /**
     * List<Song>  rank(List<Song>  songs, List<Integer>  ids)
     * s2 s1  s3 s4  s6 s5   s8 s7       1, 2, 3, 5, 9
     * s1 s2 s3 s5 s4 s6 s8 s7
     * @param args
     */
    /**
     * 重排序
     * @param songs
     * @param ids
     */
    public static void rank(List<Integer> songs, List<Integer>  ids) {
        List<Integer> rsList = new ArrayList<>();
        Map<Integer,Integer> map =  new LinkedHashMap<>();
        for (int i = 0; i < songs.size(); i++) {
            map.put(songs.get(i), songs.get(i));
        }

        for (int i = 0; i < ids.size(); i++) {
            Integer id = ids.get(i);
            if (map.containsKey(id)){
                rsList.add(map.get(id));
            }
        }
        songs.removeAll(ids);
        for (int i = 0; i < songs.size(); i++) {
            Integer songId = songs.get(i);
            if (map.containsKey(songId)){
                rsList.add(map.get(songId));
            }
        }

        for (int i = 0; i < rsList.size(); i++) {
            System.out.print(rsList.get(i)+" , ");
        }

    }


    /**
     * List<Song>  rank(List<Song>  songs, List<Integer>  ids)
     * s2 s1  s3 s4  s6 s5   s8 s7       1, 2, 3, 5, 9
     * s1 s2 s3 s5 s4 s6 s8 s7
     * @param args
     */
    public static List<Song> rank_2(List<Song> songs, List<Integer>  ids) {
        List<Song> rsSongList = new ArrayList<>();
        List<String> rsList = new ArrayList<>();
        Map<Integer,String> map =  new LinkedHashMap<>();

        List<Integer> newSongIdList = new LinkedList<>();

        for (int i = 0; i < songs.size(); i++) {
            Song song = songs.get(i);
            Integer sondId = song.getId();
            map.put(sondId, song.getName());

            if (!ids.contains(sondId)){
                newSongIdList.add(sondId);
            }
        }

        for (int i = 0; i < ids.size(); i++) {
            Integer id = ids.get(i);
            if (map.containsKey(id)){
                rsList.add(map.get(id));
                rsSongList.add(new Song(id, map.get(id)));
            }
        }

        for (int i = 0; i < newSongIdList.size(); i++) {
            Integer songId = newSongIdList.get(i);
            if (map.containsKey(songId)){
                rsList.add(map.get(songId));
                rsSongList.add(new Song(songId, map.get(songId)));
            }
        }

        for (int i = 0; i < rsList.size(); i++) {
            System.out.print(rsList.get(i)+" , ");
        }

        System.out.println("");

        for (int i = 0; i < rsSongList.size(); i++) {
            System.out.print(rsSongList.get(i).getName()+" , ");
        }
        return rsSongList;

    }
}
